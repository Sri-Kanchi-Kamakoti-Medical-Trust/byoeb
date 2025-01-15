import datetime
import sys
import yaml

import os

local_path = os.environ["APP_PATH"]
with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

sys.path.append(local_path.strip() + "/src")

NUM_EXPERTS = 1
from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB, UserRelationDB


from messenger import WhatsappMessenger
from responder import WhatsappResponder
from conversation_database import (
    LoggingDatabase
)
import traceback

userdb = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)


import pandas as pd
from tqdm import tqdm

logger = LoggingDatabase(config)
responder = WhatsappResponder(config)

to_ts = datetime.datetime.now() - datetime.timedelta(hours=4)
from_ts = datetime.datetime.now() - datetime.timedelta(hours=12)

list_cursor = user_conv_db.get_all_unresolved(from_ts, to_ts)


df = pd.DataFrame(list_cursor)

if len(df) == 0:
    print("No unresolved queries")
    sys.exit(0)

df = df[df['query_type'] != 'small-talk']
df.reset_index(drop=True, inplace=True)

def send_reminder(phone_number, body, reply_id):
    try:
        responder.messenger.send_message(
            phone_number,
            body,
            reply_id
        )
    except Exception as e:
        print(e)
        print(traceback.format_exc())

reminder_message = "Hi, this is a reminder to respond to above query",
for i, row in tqdm(df.iterrows()):
    print(row.keys())
    print(row['message_id'], row['message_english'])
    bot_message = bot_conv_db.find_with_transaction_id(row['message_id'], 'poll_primary')
    if not bot_message:
        message_id = row['message_id']
        receiver_id = bot_message['receiver_id']
        expert_data = userdb.get_from_user_id(receiver_id)
        phone_number = expert_data['phone_number']
        send_reminder(phone_number, reminder_message, message_id)
    
    bot_message = bot_conv_db.find_with_transaction_id(row['message_id'], 'poll_escalated')
    if not bot_message:
        message_id = row['message_id']
        receiver_id = bot_message['receiver_id']
        expert_data = userdb.get_from_user_id(receiver_id)
        phone_number = expert_data['phone_number']
        send_reminder(phone_number, reminder_message, message_id)
    