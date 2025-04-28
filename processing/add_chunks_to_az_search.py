import os
import json
import sys
import yaml
sys.path.append("src")
from tqdm import tqdm
from pre_verified import KnowledgeBaseClient

config_path = os.path.join(os.environ['APP_PATH'], 'config.yaml')
with open(config_path) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)



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

kb_client = KnowledgeBaseClient(
    os.environ["AZURE_SEARCH_ENDPOINT"],
    os.environ["KB_SEARCH_INDEX_NAME"]
)


print(f"Number of data chunks: {len(kb_data)}")

for i, data in tqdm(enumerate(kb_data), total=len(kb_data)):

    kb_client.add_new_data_chunk(
        id = data['id'],
        kb_data = data,
    )
