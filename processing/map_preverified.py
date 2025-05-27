import os
import sys
local_path = os.path.join(os.environ['APP_PATH'], 'src')
sys.path.append(local_path)
import json
import pandas as pd
from tqdm import tqdm
from azure_search import OpenAIEmbeddingClient
import numpy as np

kb_data_path = 'kb_data.jsonl'

def load_kb_data(file_path):
    kb_data = []
    with open(file_path, 'r') as file:
        for line in file:
            try:
                kb_data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
    return kb_data

kb_data = load_kb_data(kb_data_path)

print(kb_data[:5])  # Print first 5 entries for verification

qna_csv_path = 'preverified-qna.csv'

qna_df = pd.read_csv(qna_csv_path)

qna_df['text'] = qna_df['Query'] + ' ' + qna_df['Response']
print(qna_df.head())


openai_embedding_client = OpenAIEmbeddingClient()

kb_chunks = [item['data_chunk'] for item in kb_data]
print(f"Number of KB chunks: {len(kb_chunks)}")

# kb_chunks = kb_chunks[:5]
kb_embeddings = openai_embedding_client.get_embedding_batch(kb_chunks)

kb_embeddings = np.array(kb_embeddings)
print(f"KB Embeddings shape: {kb_embeddings.shape}")

qna_df['related_chunk_ids'] = None

for i, row in tqdm(qna_df.iterrows(), total=qna_df.shape[0]):
    qn = row['text']
    qn_embedding = openai_embedding_client.get_embedding(qn)
    qn_embedding = np.array(qn_embedding).reshape(1, -1)
    similarity = np.dot(qn_embedding, kb_embeddings.T)
    
    top_k = 3
    top_k_indices = np.argsort(similarity[0])[-top_k:]

    qna_df.at[i, 'related_chunk_ids'] = [kb_data[idx]['id'] for idx in top_k_indices]

    
qna_df.to_csv('qna_with_kb_chunk_ids.csv', index=False)