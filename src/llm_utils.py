import os
import json
import re
from utils import get_llm_response

class QueryRewriter:
    def __init__(self):
        """
        Initialize the QueryRewriter with the necessary configurations.
        """
        self.llm_prompts = json.load(open(os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "llm_prompt.json")))
    
    def _parse_xml_response(self, response):
        """
        Parse the XML-like response from the LLM.
        
        Args:
            response (str): The response from the LLM.
            
        Returns:
            dict: A dictionary containing the parsed values.
        """
        result = {}
        patterns = {
            'query_en': r'<query_en>(.*?)</query_en>',
            'query_en_addcontext': r'<query_en_addcontext>(.*?)</query_en_addcontext>',
            'query_type': r'<query_type>(.*?)</query_type>'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, response, re.DOTALL)
            if match:
                result[key] = match.group(1).strip()
        
        return result
    
    def format_conversation_history(self, conversation_history):
        """
        Format the conversation history for the LLM.
        
        Args:
            conversation_history (list): The conversation history to format.
            
        Returns:
            str: The formatted conversation history.
        """
        formatted_history = []
        for message in conversation_history:
            role = message['role']
            content = message['content']
            if role == 'user':
                formatted_history.append(f"<user>{content}</user>")
            elif role == 'assistant':
                formatted_history.append(f"<assistant>{content}</assistant>")
        return "\n".join(formatted_history)

    def translate_and_rewrite_query(self, query, src_lang, conversation_history):
        """
        Translate and rewrite the query using a language model and conversation history.
        Args:
            query (str): The original query to be translated and rewritten.
            src_lang (str): The source language of the query.
            conversation_history (str): The conversation history to provide context.
        Returns:
            dict: A dictionary containing the rewritten query and additional information.
        """
        system_prompt = self.llm_prompts["query_translate_and_rerank"]["general"]
        lang_specific_prompt = self.llm_prompts["query_translate_and_rerank"][src_lang]
        system_prompt = system_prompt.replace("<lang_specific>", lang_specific_prompt)
        conversation_history = self.format_conversation_history(conversation_history)
        print(f"Conversation History: {conversation_history}")
        query_prompt = f"<query_src>{query}</query_src>\n<conversation_history>{conversation_history}</conversation_history>"
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
        # Parse the response and return as a dictionary
        result = self._parse_xml_response(response)
        result['query_type'] = "small-talk" if result['query_type'] == "small_talk" else result['query_type']
        
        return result

if __name__ == "__main__":
    query_rewriter = QueryRewriter()
    query = "ab kya karu?"
    src_lang = "hi"
    conversation_history = [
        {"role": "user", "content": "Hello, how are you?"},
        {"role": "assistant", "content": "I'm fine, thank you! How can I assist you today?"},
        {"role": "user", "content": "I need help with my OpenAI account, my key got uploaded on github accidentally."},
        {"role": "assistant", "content": "Sure, I can help you with that."},
    ]
    rewritten_query = query_rewriter.translate_and_rewrite_query(query, src_lang, conversation_history)
    print(rewritten_query)
