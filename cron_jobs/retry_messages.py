import sys
import yaml
from azure.storage.queue import QueueClient
import os
import re
import json
local_path = os.environ["APP_PATH"]
with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

sys.path.append(local_path.strip() + "/src")

from database import UserDB, BotConvDB
import datetime
from time import sleep
from messenger import WhatsappMessenger
from app_logging import (
    LoggingDatabase
)
import traceback

MAX_RETRIES = 3  # Maximum number of retries for each message

class RetryClient:
    def __init__(self, config):
        self.config = config
        self.user_db = UserDB(config)
        self.bot_conv_db = BotConvDB(config)
        self.logger = LoggingDatabase(config)
        self.messenger = WhatsappMessenger(config, self.logger)
        self.queue_client = QueueClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip(),
            os.environ["RETRY_QUEUE_NAME"].strip()
        )

    def parse_message_content(self, message_content):
        """Parse message content, handling both old Python format and new JSON format"""
        try:
            # First try JSON parsing
            return json.loads(message_content)
        except json.JSONDecodeError:
            # If JSON fails, try to parse Python format
            try:
                # Convert Python datetime to string before eval
                # Replace datetime.datetime(...) with ISO string
                datetime_pattern = r'datetime\.datetime\((\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+)(?:,\s*(\d+))?\)'
                
                def datetime_replacer(match):
                    groups = match.groups()
                    year, month, day, hour, minute, second = [int(g) for g in groups[:6]]
                    microsecond = int(groups[6]) if groups[6] else 0
                    dt = datetime.datetime(year, month, day, hour, minute, second, microsecond)
                    return f'"{dt.isoformat()}"'
                
                # Replace datetime objects with ISO strings
                modified_content = re.sub(datetime_pattern, datetime_replacer, message_content)
                
                # Now safely evaluate the modified string
                # First make it proper JSON by replacing single quotes
                modified_content = modified_content.replace("'", '"')
                
                # Parse as JSON
                return json.loads(modified_content)
                
            except Exception as e:
                print(f"Failed to parse message content: {e}")
                print(f"Content: {message_content}")
                raise

    def retry_message_from_azure_queue(self):
        while True:
            try:
                #get num of msgs in queue
                message_count = self.queue_client.get_queue_properties().approximate_message_count
                print(f"Messages in queue: {message_count}")
                messages = self.queue_client.receive_messages(messages_per_page=1, visibility_timeout=30)
                messages = list(messages)
                
                if len(messages) == 0:
                    print("No messages to retry")
                    break
                
                # print(f"Received {len(messages)} messages to retry")
                for message in messages:
                    message_content = message.content.replace("'", "\"")  # Ensure JSON format
                    body = self.parse_message_content(message_content)
                    
                    print(f"Retrying message: {body}")
                    try:
                        user_row = self.user_db.get_from_whatsapp_id(body["user_whatsapp_id"])
                        prev_msg_obj = self.bot_conv_db.get_from_message_id(body["message_id"])

                        # print(prev_msg_obj)

                        if user_row is None or prev_msg_obj is None:
                            self.queue_client.delete_message(message)
                            continue

                        mesg_type = prev_msg_obj["message_type"]
                        prev_msg_metadata = prev_msg_obj.get("metadata", {})
                        retry_num = 0
                        if prev_msg_metadata is not None:
                            retry_num = prev_msg_metadata.get("retry_num", 0)

                        if retry_num >= MAX_RETRIES:
                            # print(f"Max retries reached for message ID {body['message_id']}. Deleting message from queue.")
                            self.queue_client.delete_message(message)
                            continue
                        
                        if mesg_type == "onboarding_template":   
                            onboarding_msg_id = self.messenger.send_template(user_row['whatsapp_id'], 'catbot_consent', user_row['user_language'])
                            # print(f"Sent onboarding template message with ID: {onboarding_msg_id}")
                            self.bot_conv_db.insert_row(
                                receiver_id=user_row['user_id'],
                                message_type='onboarding_template',
                                message_id=onboarding_msg_id,
                                audio_message_id=None,
                                message_source_lang=None,
                                message_language=user_row['user_language'],
                                message_english=None,
                                reply_id=None,
                                citations=None,
                                message_timestamp=datetime.datetime.now(),
                                transaction_message_id=None,
                                metadata={"retry_num": retry_num+1}
                            )

                        self.queue_client.delete_message(message)
                    except Exception as e:
                        print(f"Failed to send message: {e}")
                        traceback.print_exc()
            except Exception as e:
                print(f"Error receiving messages from queue: {e}")
                traceback.print_exc()


if __name__ == "__main__":
    retry_client = RetryClient(config)
    #run this script in exponential backoff mode
    # to retry messages from the Azure queue
    print("Starting to retry messages from Azure queue...")
    num_tries = 3
    backoff_time = [0, 10, 20] # minutes
    for i in range(num_tries):
        sleep_time = backoff_time[i] * 60  # convert minutes to seconds
        print(f"Attempt {i+1}/{num_tries}: Retrying messages in {sleep_time} seconds...")
        sleep(sleep_time)
        retry_client.retry_message_from_azure_queue()
        