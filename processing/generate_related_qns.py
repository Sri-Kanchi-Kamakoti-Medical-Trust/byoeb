import os
import sys
local_path = os.path.join(os.environ['APP_PATH'], 'src')
sys.path.append(local_path)
import json
from utils import get_llm_response
import pandas as pd
from tqdm import tqdm
import re

class RelatedQnGenerator:
    def __init__(self):
        self.prompts = json.load(open(os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'], 'related_qn_prompts.json')))

    def generate_related_qn(self, chunk_text):
        initial_qn = self.generate_initial_qn(chunk_text)
        verified_qn = self.verify_related_qn(chunk_text, initial_qn)
        edited_qn = self.edit_related_qn(chunk_text, verified_qn)
        return edited_qn
        
    def parse_qn(self, response):
        pattern = r'<pair_\d+>\s*<q>(.*?)</q>\s*<a>(.*?)</a>\s*</pair_\d+>'
        pairs = re.findall(pattern, response, re.DOTALL)
        
        # Clean up the extracted text by removing extra whitespace
        parsed_pairs = []
        for question, answer in pairs:
            parsed_pairs.append({
            'question': question.strip(),
            'answer': answer.strip()
            })
            
        return parsed_pairs

    def generate_initial_qn(self, chunk_text):
        sys_prompt = self.prompts['related_questions_generation_initial']
        user_prompt = f"<data_chunk>{chunk_text}</data_chunk>\n"
        prompt = [
            {
                "role": "system",
                "content": sys_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ]
        response = get_llm_response(prompt)
        initial_qn = self.parse_qn(response)
        return initial_qn

    def parse_verification_output(self, response, related_qn):
        pattern = r'<pair_(\d+)><is_grounded>(yes|no)</is_grounded><is_selfcontained>(yes|no)</is_selfcontained><is_unique>(yes|no)</is_unique><explanation>(.*?)</explanation></pair_\1>'
        pairs = re.findall(pattern, response, re.DOTALL)
        
        verified_pairs = []
        for pair_num, grounded, selfcontained, unique, explanation in pairs:
            pair_index = int(pair_num) - 1
            pair_data = related_qn[pair_index].copy()
            pair_data.update({
                'is_grounded': grounded.lower() == 'yes',
                'is_selfcontained': selfcontained.lower() == 'yes',
                'is_unique': unique.lower() == 'yes',
                'verification_explanation': explanation.strip()
            })
            verified_pairs.append(pair_data)
        return verified_pairs
    
    def verify_related_qn(self, chunk_text, related_qn):
        sys_prompt = self.prompts['related_questions_verification']
        user_prompt = f"<data_chunk>{chunk_text}</data_chunk>\n"
        user_prompt += f"<n>{len(related_qn)}</n>\n"
        pairs_text = ""
        for i, pair in enumerate(related_qn, 1):
            pairs_text += f"<pair_{i}><q>{pair['question']}</q><a>{pair['answer']}</a></pair_{i}>"
        user_prompt += f"<pairs>{pairs_text}</pairs>\n"

        prompt = [
            {
                "role": "system",
                "content": sys_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ]
        response = get_llm_response(prompt)
        verification_feedback = self.parse_verification_output(response, related_qn)
        return verification_feedback
    
    def edit_related_qn(self, chunk_text, related_qn):
        sys_prompt = self.prompts['related_questions_generation_edit']
        user_prompt = f"<data_chunk>{chunk_text}</data_chunk>\n"
        user_prompt += f"<n>{len(related_qn)}</n>\n"
        pairs_text = ""
        for i, pair in enumerate(related_qn, 1):
            is_grounded = "yes" if pair.get('is_grounded', False) else "no"
            is_selfcontained = "yes" if pair.get('is_selfcontained', False) else "no"
            is_unique = "yes" if pair.get('is_unique', False) else "no"
            explanation = pair.get('verification_explanation', '')
            pairs_text += f"<pair_{i}><q>{pair['question']}</q><a>{pair['answer']}</a><is_grounded>{is_grounded}</is_grounded><is_selfcontained>{is_selfcontained}</is_selfcontained><is_unique>{is_unique}</is_unique><explanation>{explanation}</explanation></pair_{i}>\n"
                
        user_prompt += f"<pairs>{pairs_text}</pairs>\n"

        prompt = [
            {
                "role": "system",
                "content": sys_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ]
        response = get_llm_response(prompt)
        edited_qn = self.parse_qn(response)
        return edited_qn





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

related_qn_generator = RelatedQnGenerator()
for i, item in tqdm(enumerate(kb_data), total=len(kb_data)):
    chunk_text = item['data_chunk']
    related_qn = related_qn_generator.generate_related_qn(chunk_text)
    # print(f"Original chunk: {chunk_text}")
    # print(f"Generated related questions: {related_qn}")
    
    item['metadata']['related_questions'] = related_qn

with open('kb_data.jsonl', 'w') as file:
    for item in kb_data:
        file.write(json.dumps(item) + '\n')