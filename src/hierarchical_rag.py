from datetime import datetime
import json
from utils import get_client_with_token_provider, get_client_with_key
import os
import yaml

with open("config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

llm_prompts = json.load(open(os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "llm_prompt.json")))
persist_directory = os.path.join(
    os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"]), "vectordb_hierarchy"
)
llm_client = get_client_with_key()
model = os.environ["OPENAI_API_MODEL"].strip()
general = "Generic"

def hierarchical_rag_retrieve(kb_client, query, org_id, num_chunks=3):
    retrieved_chunks = kb_client.hybrid_search(query, org_id, num_chunks)
    relevant_chunks_string = ""
    relevant_update_chunks_string = ""
    chunks = []

    chunk1 = 0
    chunk2 = 0
    for i, chunk in enumerate(retrieved_chunks):
        if "kb_update" in chunk['metadata']['source'].lower():
            relevant_update_chunks_string += (f"Chunk #{chunk2 + 1}\n{chunk['data_chunk']}\n\n")
            chunk2 += 1  
        else:
            relevant_chunks_string += f"Chunk #{chunk1 + 1}\n{chunk['data_chunk']}\n\n"
            chunk1 += 1
        chunks.append((chunk["data_chunk"], chunk["metadata"]["source"].strip(), chunk["org_id"].strip()))
    
    citations: str = "\n".join(
        [ chunk["org_id"] + '-' + chunk["metadata"]["source"] for chunk in retrieved_chunks ]
    )
    
    return relevant_chunks_string, relevant_update_chunks_string, citations, chunks

def hierarchical_rag_augment(retrieved_chunks, system_prompt, query, query_type, user_context):
    # Today's date is {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\
    date_today = datetime.now().strftime("%Y-%m-%d")
    user_role = user_context.get("user_type", "")
    user_gender = user_context.get("patient_gender", "")
    user_age = user_context.get("patient_age", "")
    date_surgery = user_context.get("patient_surgery_date", "")
    user_language = user_context.get("user_language", "en")
    
    query_prompt = f"<query_type>{query_type}</query_type>\n<query_en_addcontext>{query}</query_en_addcontext>\n\
        This query was originally asked in by a(n) <user_role>{user_role}</user_role> who is a <user_gender>{user_gender}</user_gender> aged <user_age>{user_age}</user_age> years.\
        The user speaks <user_language>{user_language}</user_language>.\n\
        The patient’s surgery is scheduled for <date_surgery>{date_surgery}</date_surgery>, and today’s date is <date_today>{date_today}</date_today>.\n\
        <raw_knowledge_base>{retrieved_chunks[0]}</raw_knowledge_base>\n<new_knowledge_base>{retrieved_chunks[1]}</new_knowledge_base>" 

    prompt = [{"role": "system", "content": system_prompt}]
    prompt.append({"role": "user", "content": query_prompt})
    return prompt

def hierarchical_rag_generate(prompt, schema=None):
    if schema is None:
        response = llm_client.chat.completions.create(
            model=model,
            messages=prompt,
            temperature=0,
        )
        response_text = response.choices[0].message.content.strip()
        return response_text
    
    response = llm_client.chat.completions.create(
        model=model,
        messages=prompt,
        temperature=0,
        response_format= { "type": "json_schema", "json_schema": schema }
    )
    response_text = response.choices[0].message.content.strip()
    return response_text


def rag(query, org_id):
    system_prompt = llm_prompts["answer_query"]
    relevant_chunks_string, relevant_update_chunks_string, citations, chunks = hierarchical_rag_retrieve(query, org_id)
    print(chunks)
    prompt = hierarchical_rag_augment("", (relevant_chunks_string, relevant_update_chunks_string), system_prompt, query)
    response = hierarchical_rag_generate(prompt)
    return response, citations, chunks

# query1 = "What are the list of Health insurance companies that hospital provides ? Share upto 3"
# org_id = "BLR"
# response, citations, chunks = rag(query1, org_id)
# print(response)