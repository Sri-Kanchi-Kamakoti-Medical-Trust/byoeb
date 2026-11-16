import os
import json
import datetime
import pymongo
import certifi
from uuid import uuid4
from cachetools import cached, TTLCache
from database.base import BaseDB

eight_hours = 1
cache = TTLCache(maxsize=100, ttl=eight_hours)

# Consent notice shown to patients (Quadra P1-3). Each version is kept in git
# (data/consent_notices/<version>.json) and copied into the consent_notices
# collection, and every consent record points at the version the patient saw.
CONSENT_NOTICE_VERSION = "v1"


def load_consent_notice(version=CONSENT_NOTICE_VERSION):
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    with open(os.path.join(repo_root, "data", "consent_notices", f"{version}.json"), encoding="utf-8") as f:
        return json.load(f)


class UserDB(BaseDB):
    def __init__(self, config):
        super().__init__(config)
        self.collection = self.db[config['COSMOS_USER_COLLECTION']]
        self.consent_notices = self.db["consent_notices"]
        self._consent_notice = None

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
    
    def get_from_user_id(self, user_id):
        user = self.collection.find_one({'user_id': user_id})
        return user

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
    
    def get_random_expert(self, expert_type, number_of_experts, org_id):
        # org_id is required so a random pick can never cross hospital units.
        pipeline = [
            {"$match": {"user_type": expert_type, "org_id": org_id}},
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

    def current_consent_notice(self):
        """The notice version in force. Also stores a copy in the database once (never overwritten).

        Returns None if the notice cannot be loaded, so onboarding is never blocked by it.
        """
        if self._consent_notice is None:
            try:
                notice = load_consent_notice()
                self.consent_notices.update_one(
                    {"version": notice["version"]},
                    {"$setOnInsert": {**notice, "stored_at": datetime.datetime.now()}},
                    upsert=True,
                )
                self._consent_notice = notice
            except Exception as e:
                print(f"Consent notice not loaded: {type(e).__name__}")
                return None
        return self._consent_notice

    def record_consent(self, user_id, given, message_id, notice_message_id, language):
        """Stores evidence of the patient's onboarding Yes/No as a consent record.

        Only adds fields to the existing user; nothing is removed or overwritten
        apart from a previous consent record for the same user.
        """
        notice = self.current_consent_notice()
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'consent_status': 'given' if given else 'declined',
                'consent_timestamp': datetime.datetime.now(),
                'consent_message_id': message_id,
                'consent_notice_message_id': notice_message_id,
                'consent_notice': 'catbot_onboarding_msg_utility',
                'consent_notice_version': notice["version"] if notice else None,
                'consent_notice_sha256': notice["sha256"] if notice else None,
                'consent_language': language,
                'consent_channel': 'whatsapp',
                'consent_purpose': 'cataract_surgery_patient_support',
            }}
        )

    def get_all_patients(self):
        patients = list(self.collection.find({'user_type': 'Patient'}))
        return patients

    def get_all_users(self):
        users = list(self.collection.find())
        return users
    