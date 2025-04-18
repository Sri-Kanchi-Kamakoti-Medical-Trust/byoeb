import os
import datetime
import pymongo
import certifi
from uuid import uuid4
from cachetools import cached, TTLCache
from database.base import BaseDB

eight_hours = 1
cache = TTLCache(maxsize=100, ttl=eight_hours)

class UserDB(BaseDB):
    def __init__(self, config):
        super().__init__(config)
        self.collection = self.db[config['COSMOS_USER_COLLECTION']]

    def insert_row(self,
        user_id,
        whatsapp_id,
        user_type,
        user_language,
        org_id = 'BLR',
        meta: dict = None):

        user = {
            'user_id': user_id,
            'whatsapp_id': str(whatsapp_id),
            'user_type': user_type,
            'user_language': user_language,
            'org_id': org_id,
            'timestamp' : datetime.datetime.now(),
        }
        if meta:
            user.update(meta)
        db_id = self.collection.insert_one(user)
        return db_id
    
    @cached(cache)
    def get_from_user_id(self, user_id):
        user = self.collection.find_one({'user_id': user_id})
        return user

    @cached(cache)
    def get_from_whatsapp_id(self, whatsapp_id):
        user = self.collection.find_one({'whatsapp_id': whatsapp_id})
        return user
    
    def get_from_user_name(self, user_name):
        user = self.collection.find_one({'user_name': user_name})
        return user
    
    def update_user_language(self, user_id, user_language):
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'user_language': user_language
            }}
        )
    
    def get_random_expert(self, expert_type, number_of_experts):
        pipeline = [
            {"$match": {"user_type": expert_type}},
            {"$sample": {"size": number_of_experts}}
        ]
        experts = list(self.collection.aggregate(pipeline))
        return experts

    def add_or_update_related_qns(self, user_id, related_qns):
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
            'related_qns': related_qns
            }},
            upsert=True
        )

    def update_conv_history(self, user_id, new_conv, history):
        history = history + [new_conv]
        history = history[-10:]  # Keep only the last 10 items
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'conv_history': history
            }}
        )

    def get_related_qns(self, user_id):
        user = self.collection.find_one({'user_id': user_id})
        return user.get('related_qns', [])
    
    def update_activity_timestamp(self, row):
        self.collection.update_one(
            {'_id': row["_id"]},
            {'$set': {
                'activity_timestamp': int(datetime.datetime.now().timestamp())
            }}
        )
        
        try:
            for key in cache.keys():
                if row['user_id'] in str(key):
                    print("invalidating cache with user_id")
                    cache.pop(key)
                if row['whatsapp_id'] in str(key):
                    print("invalidating cache with whatsapp_id")
                    cache.pop(key)
        except Exception as e:
            return

    def clear_cache(self):
        cache.clear()

    def add_or_update_onboarding_response(self, user_id, onboarded):
        #True -> Yes, False -> No, None -> No response
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'onboarded': onboarded
            }},
            upsert=True
        )

    def get_all_patients(self):
        patients = list(self.collection.find({'user_type': 'Patient'}))
        return patients

    def get_all_users(self):
        users = list(self.collection.find())
        return users
    