
import os
import sys
import traceback
import json
import re
import random
from app_logging import (
    LoggingDatabase,
)
from azure_language_tools import translator
from database import UserConvDB
from typing import Any
from utils import get_llm_response
from datetime import datetime
from azure_search import KnowledgeBaseClient
from hierarchical_rag import hierarchical_rag_augment, hierarchical_rag_generate, hierarchical_rag_retrieve

IDK = "I do not know the answer to your question"


class KnowledgeBase:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.persist_directory = os.path.join(
            os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"]), "vectordb_hierarchy"
        )
        self.kb_client = KnowledgeBaseClient(
            os.environ["AZURE_SEARCH_ENDPOINT"],
            os.environ["KB_SEARCH_INDEX_NAME"],
        )
        self.translator = translator()
        
        self.llm_prompts = json.load(open(os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "llm_prompt.json")))
    
    def hierarchical_rag_answer_query(
        self,
        row_query: dict[str, Any],
        logger: LoggingDatabase,
        row_lt: dict[str, Any],
    ):
        num_chunks_array = [3, 7]
        for num_chunks in num_chunks_array:
            print("Trying with num_chunks: ", num_chunks)
            llm_output, citations = self.hierarchical_rag_answer_query_helper(
                row_query, logger, num_chunks, row_lt
            )
            if not llm_output["response_en"].startswith(IDK):
                break

        return (llm_output, citations)

    def hierarchical_rag_answer_query_helper(
        self,
        row_query: dict[str, Any],
        logger: LoggingDatabase,
        num_chunks: int = 3,
        row_lt: dict[str, Any] = None,
    ):
        if self.config["API_ACTIVATED"] is False:
            gpt_output = "API not activated"
            citations = "NA-API"
            query_type = "small-talk"
            return (gpt_output, citations, query_type)
        
        query_context = row_query["message_context"]
        org_id = row_lt["org_id"]
        if not query_context.endswith("?"):
            query_context += "?"
        print("Query: ", query_context)
        relevant_chunks_string, relevant_update_chunks_string, citations, chunks, related_questions = hierarchical_rag_retrieve(self.kb_client, query_context, org_id, num_chunks)
        logger.add_log(
            sender_id="bot",
            receiver_id="bot",
            message_id=None,
            action_type="get_citations",
            details={"query": query_context, "chunks": chunks, "transaction_id": row_query["message_id"]},
            timestamp=datetime.now(),
        )
        relevant_chunks_tuple = (relevant_chunks_string, relevant_update_chunks_string)
        
        system_prompt = self.llm_prompts["answer_query"]["general"]
        lang_specific_prompt = self.llm_prompts["answer_query"][row_lt["user_language"]]
        system_prompt = system_prompt.replace("<lang_specific>", lang_specific_prompt)
        prompt = hierarchical_rag_augment(
            relevant_chunks_tuple,
            system_prompt, 
            query = row_query["message_context"],
            query_type= row_query["query_type"],
            user_context=row_lt            
        )
        logger.add_log(
            sender_id="bot",
            receiver_id="gpt4",
            message_id=None,
            action_type="answer_query_request",
            details={
                "system_prompt": prompt[0]["content"],
                "query_prompt": prompt[1]["content"],
                "transaction_id": row_query["message_id"],
            },
            timestamp=datetime.now(),
        )
        
        for _ in range(3):
            try:
                llm_output = hierarchical_rag_generate(prompt)
                llm_output = self.parse_llm_output(llm_output)
                break
            except Exception as e:
                print("Error: ", e)
                print(traceback.format_exc())
                continue
        
        logger.add_log(
            sender_id="gpt4",
            receiver_id="bot",
            message_id=None,
            action_type="answer_query_response",
            details={
                "system_prompt": prompt[0]["content"],
                "query_prompt": prompt[1]["content"],
                "gpt_output": llm_output,
                "transaction_id": row_query["message_id"],
            },
            timestamp=datetime.now(),
        )

        # Fetch grounded related questions

        llm_output["related_questions_en"] = []
        llm_output["related_questions_src"] = []
        random.shuffle(related_questions)
        for i, question in enumerate(related_questions):
            if i >= 3:
                break
            llm_output["related_questions_en"].append(question)

        llm_output["related_questions_src"] = self.translator.translate_text_batch(
            llm_output["related_questions_en"],
            "en",
            row_lt["user_language"],
        )

        return llm_output, citations

    def get_summarize_long_response_prompt(self, response):
        system_prompt = f"""Please summarise the given answer in 700 characters or less. Only return the summarized answer and nothing else.\n"""
            
        query_prompt = f"""You are given the following response: {response}"""
        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})
        return prompt
        
    def parse_llm_output(self, output):
        result = {}

        # Define regex patterns for each field
        patterns = {
            'response_en': r'<response_en>(.*?)</response_en>',
            'response_src': r'<response_src>(.*?)</response_src>',
            'related_questions_en': r'<related_questions_en>(.*?)</related_questions_en>',
            'related_questions_src': r'<related_questions_src>(.*?)</related_questions_src>'
        }

        # Extract each field using regex
        for key, pattern in patterns.items():
            match = re.search(pattern, output, re.DOTALL)
            if match:
                result[key] = match.group(1).strip()

        # not needed now, reading grounded questions from search client
        # Further parse related questions 
        # if 'related_questions_en' in result:
        #     result['related_questions_en'] = re.findall(r'<q-\d+>(.*?)</q-\d+>', result['related_questions_en'], re.DOTALL)
        # if 'related_questions_src' in result:
        #     result['related_questions_src'] = re.findall(r'<q-\d+>(.*?)</q-\d+>', result['related_questions_src'], re.DOTALL)

        return result

    def generate_correction(
        self,
        row_query: dict[str, Any],
        row_response: dict[str, Any],
        row_correction: dict[str, Any],
        logger: LoggingDatabase,
    ):
        
        if self.config["API_ACTIVATED"] is False:
            gpt_output = "API not activated"
            return gpt_output

        system_prompt = self.llm_prompts["generate_correction"]
        query = row_query["message_english"]
        response = row_response["message_english"]
        correction = row_correction["message"]
        query_prompt = f"""
        A user asked the following query:\n\
                "{query}"\n\
            A chatbot answered the following:\n\
            "{response}"\n\
            An expert corrected the response as follows:\n\
            "{correction}"\n\

        """
        transaction_message_id = row_query["message_id"]
        logger.add_log(
            sender_id="bot",
            receiver_id="bot",
            message_id=None,
            action_type="get_correction",
            details={"system_prompt": system_prompt, "query_prompt": query_prompt, "transaction_message_id": transaction_message_id},
            timestamp=datetime.now(),
        )

        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})

        gpt_output = get_llm_response(prompt)

        if len(gpt_output) < 700:
            return gpt_output
        else:
            system_prompt = f"""Please summarise the provided answer in 700 characters or less. Only return the summarized answer and nothing else.\n"""
            query_prompt = f"""You are given the following response: {gpt_output}"""
            prompt = [{"role": "system", "content": system_prompt}]
            prompt.append({"role": "user", "content": query_prompt})

            logger.add_log(
                sender_id="bot",
                receiver_id="bot",
                message_id=None,
                action_type="gpt4",
                details={"system_prompt": system_prompt, "query_prompt": query_prompt},
                timestamp=datetime.now(),
            )
            gpt_output = get_llm_response(prompt)

            return gpt_output

    def follow_up_questions(
        self,
        query: str,
        response: str,
        user_type: str,
        logger: LoggingDatabase,
    ) -> list[str]:
        """look at the chat history and suggest follow up questions

        Args:
            query (str): the query
            response (str): the response from the bot
            llm (OpenAI): an OpenAI model

        Returns:
            list[str]: a list of potential follow up questions
        """

        if self.config["API_ACTIVATED"] is False:
            print("API not activated")
            return ["Q1", "Q2", "Q3"]
        
        schema = {
            "name": "response_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "items": {
                            "type": "string",
                        }
                    }
                },
                "required": ["questions"]
            }
        }
        system_prompt = self.llm_prompts["follow_up_questions"]
        query_prompt = f"""
            A user asked the following query:\n\
                    "{query}"\n\
                A chatbot answered the following:\n\
                "{response}"\n\
            """

        prompt = [{"role": "system", "content": system_prompt}]
        prompt.append({"role": "user", "content": query_prompt})

        llm_out = get_llm_response(prompt, schema)
        json_output = json.loads(llm_out.strip())
        print(llm_out)
        next_questions = json_output["questions"]

        logger.add_log(
            sender_id="bot",
            receiver_id="bot",
            message_id=None,
            action_type="gpt4",
            details={
                "system_prompt": system_prompt,
                "query_prompt": query_prompt,
                "gpt_output": llm_out,
            },
            timestamp=datetime.now(),
        )

        return next_questions
