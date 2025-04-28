import os
import re
import json
from azure.search.documents import SearchClient
from utils import get_client_with_key
from azure.identity import DefaultAzureCredential
from azure.search.documents.models import VectorizableTextQuery
from utils import get_llm_response
from datetime import datetime, timezone

class OpenAIEmbeddingClient:
    def __init__(self):
        self.client = get_client_with_key()
        self.model = os.getenv("OPENAI_API_EMBED_MODEL")

    def get_embedding(self, text):
        response = self.client.embeddings.create(
            input=text,
            model=self.model,
        )
        return response.data[0].embedding
    
    def get_embedding_batch(self, texts):
        response = self.client.embeddings.create(
            input=texts,
            model=self.model,
        )
        return [data.embedding for data in response.data]


class PreverifiedClient:
    def __init__(self, endpoint, index_name):
        self.client = SearchClient(endpoint=endpoint,
                                   index_name=index_name,
                                   credential=DefaultAzureCredential())
        self.openai_embedding_client = OpenAIEmbeddingClient()
        self.llm_prompts = json.load(open(os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "llm_prompt.json")))

    def add_new_qa(self,
        id: str,
        question: str,
        answer: str,
        org_id: str = None,
    ):
        question_answer = f"{question} {answer}"
        question_vector, question_answer_vector = self.openai_embedding_client.get_embedding_batch([question, question_answer])
        self.client.upload_documents(documents=[{
            "id": id,
            "question": question,
            "question_answer": question_answer,
            "question_vector": question_vector,
            "question_answer_vector": question_answer_vector,
            "metadata": {"answer": answer},
            "org_id": org_id,
        }])

    def add_new_qa_batch(self, qa_pairs):
        documents = []
        for i, (question, answer) in enumerate(qa_pairs):
            question_answer = f"{question} {answer}"
            question_vector, question_answer_vector = self.openai_embedding_client.get_embedding_batch([question, question_answer])
            documents.append({
                "id": str(i),
                "question": question,
                "question_answer": question_answer,
                "question_vector": question_vector,
                "question_answer_vector": question_answer_vector,
                "metadata": {"answer": answer},
            })
        self.client.upload_documents(documents=documents)

    def hybrid_search(self, query, org_id, k=10):
        vector_query = VectorizableTextQuery(
            text=query,
            k_nearest_neighbors=k,
            fields='question_vector',
        )
        result = self.client.search(
            search_text=query,
            vector_queries=[vector_query],
            filter="org_id eq 'Generic' or org_id eq '{}'".format(org_id),
            top=k
        )
        return [doc for doc in result]
    
    def lexical_search(self, query, org_id, k=10):
        result = self.client.search(
            search_text=query,
            filter="org_id eq 'Generic' or org_id eq '{}'".format(org_id),
            top=k,
            include_total_count=True,
            query_type="full",
        )
        return [doc for doc in result]
    
    def vector_search(self, query, org_id, k=10):
        vector_query = VectorizableTextQuery(
            text=query,
            k_nearest_neighbors=k,
            fields='question_vector',
        )
        result = self.client.search(
            vector_queries=[vector_query],
            filter="org_id eq 'Generic' or org_id eq '{}'".format(org_id),
            top=k
        )
        return [doc for doc in result]
    
    def filter_questions(self, query, results):
        filtered_results = []
        system_prompt = self.llm_prompts['preverified_filter']
        query_prompt = f"""<query_en_addcontext>{query}</query_en_addcontext>\n  
            <n>{len(results)}</n>\n 
        """
        for i, result in enumerate(results):
            query = result['question']
            query_prompt += f"""<question_{i+1}>{query}</question_{i+1}>\n"""
        
        prompt = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": query_prompt
            }
        ]
        response = get_llm_response(prompt)
        pattern = re.compile(r"<query_(\d+)_binary>(YES|NO)</query_\d+_binary>")
        matches = pattern.findall(response)
        for match in matches:
            response_id = int(match[0])
            binary = match[1]
            if binary == "YES":
                filtered_results.append(results[response_id - 1])
        
        return filtered_results
    

    
    def rerank(self, query, results):
        system_prompt = self.llm_prompts['preverified_rerank']
        query_prompt = f"""<query_en_addcontext>{query}</query_en_addcontext>\n  
            <n>{len(results)}</n>\n 
        """
        for i, result in enumerate(results):
            response = result['metadata']['answer']
            query_prompt += f"""<response_{i+1}>{response}</response_{i+1}>\n"""
        
        prompt = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": query_prompt
            }
        ]
        response = get_llm_response(prompt)
        rankings = {}
        pattern = re.compile(r"<response_(\d+)_rank>(\d+)</response_\d+_rank>")
        matches = pattern.findall(response)
        
        for match in matches:
            response_id = int(match[0])
            rank = int(match[1])
            rankings[response_id] = rank
        
        reranked_results = []
        for i, result in enumerate(results):
            response_id = i + 1
            if response_id in rankings:
                reranked_results.append((rankings[response_id], result))
        # Sort by rank
        reranked_results.sort(key=lambda x: x[0])
        # Return the sorted results
        return [result[1] for result in reranked_results]

    def find_closest_preverified_pair(self, query, org_id):
        preverified_pairs_top_k = self.hybrid_search(query, org_id)
        preverified_pairs_top_k = [ p for p in preverified_pairs_top_k if len(p['metadata']['answer']) < 800 ]
        preverified_pairs_top_k = self.filter_questions(query, preverified_pairs_top_k)
        preverified_pairs_reraanked = self.rerank(query, preverified_pairs_top_k)
        return preverified_pairs_reraanked[0] if preverified_pairs_reraanked else None

class KnowledgeBaseClient:
    def __init__(self, endpoint, index_name):
        self.client = SearchClient(endpoint=endpoint,
                                   index_name=index_name,
                                   credential=DefaultAzureCredential())
        self.openai_embedding_client = OpenAIEmbeddingClient()
        
    def add_new_data_chunk(self,
        id: str,
        kb_data: dict,
    ):
        data_chunk_text = kb_data['data_chunk']
        data_chunk_vector = self.openai_embedding_client.get_embedding(data_chunk_text)
        self.client.merge_or_upload_documents(documents=[{
            "id": id,
            "data_chunk": data_chunk_text,
            "data_chunk_vector": data_chunk_vector,
            "metadata" : {
                "related_questions": kb_data["metadata"]['related_questions'],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": kb_data["metadata"]['source'],
            },
            "org_id": kb_data["org_id"],
        }])

    def add_new_data_chunk_batch(self, ids, kb_data_list):
        documents = []
        data_chunks = [ data['data_chunk'] for data in kb_data_list ]
        data_chunk_vectors = self.openai_embedding_client.get_embedding_batch(data_chunks)
        for i, kb_data in enumerate(kb_data_list):
            documents.append({
                "id": ids[i],
                "data_chunk": kb_data['data_chunk'],
                "data_chunk_vector": data_chunk_vectors[i],
                "metadata" : {
                    "related_questions": kb_data["metadata"]['related_questions'],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": kb_data["metadata"]['source'],
                },
                "org_id": kb_data["org_id"],
            })

        self.client.merge_or_upload_documents(documents=documents)

    def hybrid_search(self, query, org_id, k=3):
        vector_query = VectorizableTextQuery(
            text=query,
            k_nearest_neighbors=k,
            fields='data_chunk_vector',
        )
        result = self.client.search(
            search_text=query,
            vector_queries=[vector_query],
            filter="org_id eq 'Generic' or org_id eq '{}'".format(org_id),
            top=k
        )
        return [doc for doc in result]

if __name__ == "__main__":
    
    # preverified_client = PreverifiedClient(
    #     os.environ["AZURE_SEARCH_ENDPOINT"],
    #     os.environ["PREVERIFIED_SEARCH_INDEX_NAME"]
    # )

    # question = "How long does it take to recover from cataract surgery?"
    # answer = "It usually takes about 2 weeks to fully recover from cataract surgery."
    # org_id = "TEST"

    # test_query = "What should I eat before my surgery?"

    # print(preverified_client.find_closest_preverified_pair(test_query, org_id)['question_answer'])

    kb_client = KnowledgeBaseClient(
        os.environ["AZURE_SEARCH_ENDPOINT"],
        os.environ["KB_SEARCH_INDEX_NAME"]
    )

    query = "How safe is cataract surgery?"
    org_id = "TEST"
    result = kb_client.hybrid_search(query, org_id)
    for doc in result:
        print(doc['data_chunk'])
        print(doc['metadata']['source'])
        print(doc['org_id'])
        print()