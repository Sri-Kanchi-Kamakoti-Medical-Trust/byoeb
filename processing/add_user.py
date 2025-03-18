import pymongo
import certifi
import os
import yaml

config_path = os.path.join(os.environ['APP_PATH'], "config.yaml")
with open(config_path, 'r') as data:
    config = yaml.safe_load(data)

import sys
sys.path.append(os.path.join(os.environ['APP_PATH'], 'src'))


from database.user_db import UserDB
from database.user_relation_db import UserRelationDB



user_db = UserDB(config)
user_relation_db = UserRelationDB(config)

print(config["COSMOS_DB_NAME"])
user_whatsapp_id = '919876543210'
user_language = 'en'
user_type = 'Patient'

expert_whatsapp_id = '919876543210'
expert_language = 'en'
expert_type = 'Doctor'

#assign user_id and expert_id, use uuid

from uuid import uuid4



user_row = user_db.get_from_whatsapp_id(user_whatsapp_id)
if user_row is None:
    user_id = str(uuid4())
    user_db.insert_row(user_id, user_whatsapp_id, user_type, user_language)
else:
    user_id = user_row['user_id']


expert_row = user_db.collection.find_one({'$and': [{'whatsapp_id': expert_whatsapp_id}, {'user_type': expert_type}]})
print(expert_row)
if expert_row is None:
    expert_id = str(uuid4())
    user_db.insert_row(expert_id, expert_whatsapp_id, expert_type, expert_language)
else:
    expert_id = expert_row['user_id']

user_relation_db.insert_row(user_id, expert_id, user_type, expert_type)

hyd_users = user_db.collection.find({'org_id': 'HYD'})
hyd_users = list(hyd_users)
print(hyd_users)