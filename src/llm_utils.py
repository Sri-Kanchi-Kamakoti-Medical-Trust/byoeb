import os
import json
import yaml
from app_logging import LoggingDatabase

class QueryRewriter:
    def __init__(self):
        """
        Initialize the QueryRewriter with the necessary configurations.
        """
        self.llm_prompts = json.load(open(os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "llm_prompt.json")))

    def translate_and_rewrite_query(self, query, src_lang, conversation_history, logger):
        """
        Translate and rewrite the query using a language model and conversation history.
        Args:
            query (str): The original query to be translated and rewritten.
            src_lang (str): The source language of the query.
            conversation_history (str): The conversation history to provide context.
            logger (Logger): The logger instance to log the process.
        Returns:
            str: The rewritten query in English.
        """
        return query

if __name__ == "__main__":
    query_rewriter = QueryRewriter()
    query = "How to generate API key?"
    src_lang = "en"
    conversation_history = [
        {"role": "user", "content": "Hello, how are you?"},
        {"role": "assistant", "content": "I'm fine, thank you! How can I assist you today?"},
        {"role": "user", "content": "I need help with my OpenAI account."},
        {"role": "assistant", "content": "Sure, I can help you with that."},
    ]
    logger = LoggingDatabase(config)
    rewritten_query = query_rewriter.translate_and_rewrite_query(query, src_lang, conversation_history, logger)
    print(rewritten_query)
