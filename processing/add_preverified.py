import os
import sys
local_path = os.path.join(os.environ['APP_PATH'], 'src')
sys.path.append(local_path)
import json
import pandas as pd
from tqdm import tqdm
from azure_search import OpenAIEmbeddingClient, PreverifiedClient
import numpy as np
import ast
import hashlib
import time

def generate_id(text):
    """Generate a unique ID for a QnA pair based on its content"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def main():
    # Load the QnA data from CSV
    print("Loading QnA data from CSV...")
    df = pd.read_csv('qna_with_kb_chunk_ids.csv')
    
    # Initialize PreverifiedClient
    print("Initializing PreverifiedClient...")
    preverified_client = PreverifiedClient(
        endpoint=os.environ["AZURE_SEARCH_ENDPOINT"],
        index_name=os.environ["PREVERIFIED_SEARCH_INDEX_NAME"]
    )
    
    # Process and add QnA pairs to search index
    print(f"Processing {len(df)} QnA pairs...")
    
    # Track successful and failed additions
    successful = 0
    failed = 0
    
    for index, row in tqdm(df.iterrows(), total=len(df)):
        try:
            # Extract question and answer
            question = row['Query']
            answer = row['Response']
            
            # Generate a unique ID for this QnA pair
            qna_id = generate_id(f"{question}_{answer}")
            
            # Check if the question contains Bangalore-specific information
            is_bangalore_specific = row.get('Bangalore specific? (Y - Maybe, M - Maybe, N - No)', 'N')
            
            # Set organization ID based on specificity
            org_id = "BLR" if is_bangalore_specific in ['Y', 'M'] else "Generic"
            
            # Anonymize the QnA pair to remove any PII
            # anonymized_question, anonymized_answer = preverified_client.anonymyze_qa_pair(question, answer)
            
            # Add QnA pair to the search index
            preverified_client.add_new_qa(
                id=qna_id,
                question=question,
                answer=answer,
                related_chunk_ids=ast.literal_eval(row['related_chunk_ids']),
                org_id=org_id
            )
            
            successful += 1
            
            # Add a small delay to prevent rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error processing row {index}: {e}")
            failed += 1
    
    print(f"Finished processing QnA pairs.")
    print(f"Successfully added: {successful}")
    print(f"Failed to add: {failed}")

if __name__ == "__main__":
    main()