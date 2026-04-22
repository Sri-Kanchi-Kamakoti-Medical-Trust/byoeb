import os
import yaml
import json
local_path = os.environ['APP_PATH']
import sys
from uuid import uuid4
sys.path.append(local_path + '/src')
with open(os.path.join(local_path,'config.yaml')) as file:    
    config = yaml.load(file, Loader=yaml.FullLoader)

data_path = os.path.join(local_path, os.environ['DATA_PATH'], "documents")
folders_to_process = ['SMG']

kb_data = []

for folder in folders_to_process:
    folder_path = os.path.join(data_path, folder)
    for file_name in os.listdir(folder_path):
        #read txt files and create data chunks
        if file_name.endswith('.txt'):
            with open(os.path.join(folder_path, file_name), 'r') as file:
                data = file.read()
                #divide data into chunks of 1000 characters with an overlap of 200 characters
                chunk_size = 1000
                overlap = 200
                start = 0
                while start < len(data):
                    end = start + chunk_size
                    chunk = data[start:end]
                    kb_data.append({
                        "id": str(uuid4()),
                        "data_chunk": chunk,
                        "org_id": folder,
                        "metadata": {
                            "source": file_name,
                            "related_questions": []
                        }
                    })
                    start += chunk_size - overlap

print(f"Number of data chunks: {len(kb_data)}")

#save to jsonl file
with open('kb_data.jsonl', 'w') as file:
    for data in kb_data:
        json.dump(data, file)
        file.write('\n')


