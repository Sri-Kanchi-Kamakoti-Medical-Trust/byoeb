from typing import Any
from io import BytesIO

import os
from azure_language_tools import translator
import subprocess
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import numpy as np
import json
from knowledge_base import KnowledgeBase
from app_logging import (
    LoggingDatabase,
)
import random
from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB, UserRelationDB
from messenger.whatsapp import WhatsappMessenger
import utils
from utils import remove_extra_voice_files
from onboard import onboard_wa_helper
from responder.base import BaseResponder
from azure.identity import DefaultAzureCredential
from azure_search import PreverifiedClient
from llm_utils import QueryRewriter
from hashlib import md5

IDK = "I do not know the answer to your question"

class WhatsappResponder(BaseResponder):
    def __init__(self, config):
        self.config = config
        self.knowledge_base = KnowledgeBase(config)
        self.logger = LoggingDatabase(config)
        self.messenger = WhatsappMessenger(config, self.logger)
        self.azure_translate = translator()

        self.user_db = UserDB(config)
        self.user_relation_db = UserRelationDB(config)
        self.preverified_client = PreverifiedClient(
            os.environ["AZURE_SEARCH_ENDPOINT"],
            os.environ["PREVERIFIED_SEARCH_INDEX_NAME"]
        )
        self.query_rewriter = QueryRewriter()
        self.user_conv_db = UserConvDB(config)
        self.bot_conv_db = BotConvDB(config)
        self.expert_conv_db = ExpertConvDB(config)

        self.welcome_messages = json.load(
            open(os.path.join(os.environ['DATA_PATH'], "onboarding/welcome_messages.json"), "r")
        )
        self.language_prompts = json.load(
            open(os.path.join(os.environ['DATA_PATH'], "onboarding/language_prompts.json"), "r")
        )
        self.onboarding_questions = json.load(
            open(os.path.join(os.environ['DATA_PATH'], "onboarding/suggestion_questions.json"), "r")
        )
        self.template_messages = json.load(
            open(os.path.join(os.environ['DATA_PATH'], "template_messages.json"), "r")
        )
        self.unit_contact = json.load(
            open(os.path.join(os.environ['DATA_PATH'], "unit_contact.json"), "r")
        )
        self.yes_responses = [
            self.language_prompts[key]
            for key in self.language_prompts.keys()
            if key[-3:] == "yes"
        ]
        self.no_responses = [
            self.language_prompts[key]
            for key in self.language_prompts.keys()
            if key[-2:] == "no"
        ]

        self.category_to_expert = {}

        for expert in self.config["EXPERTS"]:
            self.category_to_expert[self.config["EXPERTS"][expert]] = expert

    def check_user_type(self, from_number):
        row = self.user_db.get_from_whatsapp_id(from_number)

        if row:
            self.user_db.update_activity_timestamp(row)
            return row["user_type"], row
            
        return None, None

    def update_kb(self):
        self.knowledge_base = KnowledgeBase(self.config)

    def clear_cache(self):
        self.user_db.clear_cache()

    def response(self, body):
        if (
            body.get("object")
            and body.get("entry")
            and body["entry"][0].get("changes")
            and body["entry"][0]["changes"][0].get("value")
            and body["entry"][0]["changes"][0]["value"].get("messages")
            and body["entry"][0]["changes"][0]["value"]["messages"][0]
        ):
            pass
        else:
            self.logger.add_log(
                sender_id="whatsapp_api",
                receiver_id="Bot",
                message_id=None,
                action_type="webhook received",
                details={"body": body},
                timestamp=datetime.now(),
            )
            return

        # print("Entering response function")
        
        msg_object = body["entry"][0]["changes"][0]["value"]["messages"][0]
        from_number = msg_object["from"]
        msg_id = msg_object["id"]
        msg_type = msg_object["type"]

        print("Message object: ", msg_object)


        if self.user_conv_db.get_from_message_id(msg_id) or self.bot_conv_db.get_from_message_id(msg_id) or self.expert_conv_db.get_from_message_id(msg_id):
            # print("Message already processed", datetime.now())
            return

        user_type, row_lt = self.check_user_type(from_number)
        # print("User type: ", user_type, "Row: ", row_lt)
        if user_type is None:
            self.messenger.send_message(
                from_number,
                self.template_messages["unknown_user"]["en"],
                reply_to_msg_id=msg_id,
            )
            return


        unsupported_types = ["image", "document", "video", "location", "contacts"]
        if msg_type in unsupported_types:
            self.handle_unsupported_msg_types(msg_object, row_lt)
            return

        if msg_object.get("context", False) and msg_object["context"].get("id", False):
            reply_id = msg_object["context"]["id"]
            context_row = self.bot_conv_db.get_from_message_id(reply_id)
            if context_row is not None:
                if context_row['message_type'] == 'onboarding_template':
                    self.onboard_response(msg_object, row_lt)
                    return
                
                if context_row['message_type'] == 'lang_poll_onboarding':
                    self.language_onboarding_response(msg_object, row_lt)



        if user_type in self.config["USERS"]:
            self.handle_response_user(msg_object, row_lt)
        elif user_type in self.config["EXPERTS"]:
            self.handle_response_expert(msg_object, row_lt)

        return

    def handle_unsupported_msg_types(self, msg_object, row_lt):
        # print("Handling unsupported message types")
        msg_id = msg_object["id"]
        self.logger.add_log(
            sender_id=row_lt['whatsapp_id'],
            receiver_id="bot",
            message_id=msg_id,
            action_type="unsupported message format",
            details={"text": msg_object["type"], "reply_to": None},
            timestamp=datetime.now(),
        )
        text = self.template_messages["not_audio_or_text"]["en"]
        
        translated_text = self.template_messages["not_audio_or_text"][row_lt['user_language']]
        self.messenger.send_message(
            row_lt['whatsapp_id'], translated_text, reply_to_msg_id=msg_id
        )
        return
    
    def onboard_response(self, msg_object, row_lt):
        user_type = row_lt["user_type"]
        msg_id = msg_object["id"]
        reply_id = msg_object["context"]["id"]

        if msg_object["button"]["payload"] in self.yes_responses:
            onboard_wa_helper(self.config, self.logger, row_lt['whatsapp_id'], user_type, row_lt['user_language'], row_lt['user_id'], self.user_db)
        else:
            text_message = self.template_messages["offboarding"]["en"]
            text_message = text_message.replace("<phone_number>", self.unit_contact["phone_number"][row_lt['org_id']])
            text = self.template_messages["offboarding"][row_lt['user_language']]
            text = text.replace("<phone_number>", self.unit_contact["phone_number"][row_lt['org_id']])
            
            self.messenger.send_message(row_lt['whatsapp_id'], text, reply_to_msg_id=None)
        self.logger.add_log(
            sender_id=row_lt['whatsapp_id'],
            receiver_id="bot",
            message_id=msg_id,
            action_type="onboard",
            details={"text": msg_object["button"]["text"], "reply_to": reply_id},
            timestamp=datetime.now(),
        )

        message_english = "Yes" if msg_object["button"]["text"] in self.yes_responses else "No"

        self.user_conv_db.insert_onboarding_response(
            user_id=row_lt['user_id'],
            message_id=msg_id,
            message_type='onboarding_response',
            message_source_lang=msg_object["button"]["text"],
            source_language=row_lt['user_language'],
            message_english=message_english,
            message_timestamp=datetime.now(),
        )

        self.user_db.add_or_update_onboarding_response(
            user_id=row_lt['user_id'],
            onboarded=True if message_english == "Yes" else False,
        )

        return
    
    def language_onboarding_response(self, msg_object, row_lt):
        language_parser = {
            'English': 'en',
            'हिंदी': 'hi',
            'ಕನ್ನಡ': 'kn',
            'தமிழ்': 'ta',
            'తెలుగు': 'te',
            'اردو': 'ur',
        }
        detected_lang = language_parser[msg_object['button']['payload']]

        self.user_db.update_user_language(row_lt['user_id'], detected_lang)
        self.user_db.clear_cache()
        onboarding_msg_id = self.messenger.send_template(row_lt['whatsapp_id'], 'catbot_consent', detected_lang)
        
        self.user_conv_db.insert_lang_poll_response(
            user_id=row_lt['user_id'],
            message_id=msg_object['id'],
            message_type='lang_poll_response',
            message_source_lang=msg_object['button']['payload'],
            source_language=detected_lang,
            message_timestamp=datetime.now(),
        )


        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type='onboarding_template',
            message_id=onboarding_msg_id,
            audio_message_id=None,
            message_source_lang=None,
            message_language=detected_lang,
            message_english=None,
            reply_id=None,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=None,
        )
        return




    def expert_reminder_response(self, msg_object, row_lt):
        msg_id = msg_object["id"]
        reply_id = msg_object["context"]["id"]
        self.messenger.send_message(row_lt['whatsapp_id'], "Thank you for your response.", None)
        self.logger.add_log(
            sender_id=row_lt['whatsapp_id'],
            receiver_id="bot",
            message_id=msg_id,
            action_type="expert reminder response",
            details={"text": msg_object["context"]["id"], "reply_to": reply_id},
            timestamp=datetime.now(),
        )
        return

    def check_expiration(self, row_lt):
        patient_surgery_date = row_lt.get("patient_surgery_date", None)
        if patient_surgery_date is None:
            return False
        
        import pandas as pd
        patient_surgery_date = pd.to_datetime(patient_surgery_date)
        patient_surgery_date = datetime.strptime(str(patient_surgery_date), "%Y-%m-%d %H:%M:%S")

        if (datetime.now() - patient_surgery_date).days > 30:
            return True
        return False
    
    def handle_expired_user_message(self, msg_object, row_lt):

        # Handle expired user message by logging information and recording in database
        msg_id = msg_object["id"]
        from_number = msg_object["from"]
        msg_type = msg_object["type"]

        if msg_type == "text" or msg_type == "interactive":
            blob_name = None
            if msg_type == "text":
                message_text = msg_object["text"]["body"]
            else:
                if msg_object["interactive"]["type"] == "button_reply":
                    message_text = f"{msg_object['interactive']['button_reply']['id']}: {msg_object['interactive']['button_reply']['title']}"
                elif msg_object["interactive"]["type"] == "list_reply":
                    message_text = f"{msg_object['interactive']['list_reply']['id']}: {msg_object['interactive']['list_reply']['title']} {msg_object['interactive']['list_reply']['description']}"
        elif msg_type == "audio":
            audio_input_file = "test_audio_input.ogg"
            audio_output_file = "test_audio_output.aac"
            utils.remove_extra_voice_files(audio_input_file, audio_output_file)
            self.messenger.download_audio(msg_object, audio_input_file)
            subprocess.call(
                ["ffmpeg", "-i", audio_input_file, audio_input_file[:-3] + "wav"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING").strip()
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_name = self.config["AZURE_BLOB_CONTAINER_NAME"].strip()

            blob_name = str(datetime.now()) + "_" + str(row_lt['whatsapp_id']) + ".ogg"
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )
            with open(file=audio_input_file, mode="rb") as data:
                blob_client.upload_blob(data)
            
            message_text = self.azure_translate.speech_to_text(audio_input_file[:-3] + "wav", row_lt['user_language'], self.logger)    
        
        message_english = self.azure_translate.translate_text(
            message_text, row_lt['user_language'], "en", self.logger
        )
        
        self.user_conv_db.insert_user_query(
            user_id=row_lt['user_id'],
            message_id=msg_id,
            message_type=msg_type,
            message_source_lang=message_text,
            source_language=row_lt['user_language'],
            message_english=message_english,
            message_context={},
            query_type="expired_access",
            audio_blob_path=blob_name,
            message_timestamp=datetime.now()
        )

        message_text = self.template_messages["access_expiration"]["en"]
        message_text = message_text.replace("<phone_number>", self.unit_contact["phone_number"][row_lt['org_id']])
        source_lang = self.template_messages["access_expiration"][row_lt['user_language']]
        text = source_lang.replace("<phone_number>", self.unit_contact["phone_number"][row_lt['org_id']])
        self.messenger.send_message(row_lt['whatsapp_id'], text, msg_object["id"])
        
        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="access_expiration",
            message_id=msg_object["id"],
            audio_message_id=None,
            message_source_lang=text,
            message_language=row_lt['user_language'],
            message_english=message_text,
            reply_id=None,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=msg_object["id"],
        )
        
        return
        

    def handle_language_poll_response(self, msg_object, row_lt):
        # print("Handling language poll response")
        msg_id = msg_object["id"]
        lang_detected = msg_object["interactive"]["list_reply"]["id"][5:-1].lower()
            

        self.logger.add_log(
            sender_id=row_lt['whatsapp_id'],
            receiver_id="bot",
            message_id=msg_id,
            action_type="language_poll_response",
            details={
                "text": msg_object["interactive"]["list_reply"]["title"],
                "reply_to": msg_object["context"]["id"],
            },
            timestamp=datetime.now(),
        )
        
        for message in self.welcome_messages["users"][lang_detected]:
            self.messenger.send_message(row_lt['whatsapp_id'], message)
        audio_file = (
            "onboarding/welcome_messages_users_"
            + lang_detected
            + ".aac"
        )
        audio_file = os.path.join(os.environ['DATA_PATH'], audio_file)
        self.messenger.send_audio(audio_file, row_lt['whatsapp_id'])
        # print("Sending language poll")
        self.messenger.send_language_poll(
            row_lt['whatsapp_id'],
            self.language_prompts[lang_detected],
            self.language_prompts[lang_detected + "_title"],
        )

        self.user_db.update_user_language(row_lt['user_id'], lang_detected)
        return
    
    def send_query_response(self, msg_type, msg_id, llm_response, row_lt):

        response = llm_response["response_en"]
        response_source = llm_response["response_src"]
        audio_msg_id = None
        sent_msg_id = self.messenger.send_message(
            row_lt['whatsapp_id'], response_source, msg_id
        )
        if msg_type == "audio":
            audio_output_file = "test_audio_input.ogg"
            self.azure_translate.text_to_speech(
                response_source, row_lt['user_language']+'-IN', audio_output_file
            )
            audio_msg_id = self.messenger.send_audio(
                audio_output_file, row_lt['whatsapp_id'], msg_id
            )
            utils.remove_extra_voice_files(None, audio_output_file)

        return sent_msg_id, audio_msg_id, response_source

    def send_audio_idk_response(self, row_lt, row_query):
        msg_id = row_query['message_id']
        idk_message_template = self.template_messages["idk"][f"{row_lt['user_language']}"]["audio"]
        idk_message = idk_message_template.replace("<query>", row_query['message_source_lang'])
        options = self.template_messages["idk"][row_lt['user_language']]["audio_options"]
        sent_msg_id = self.messenger.send_message_with_options(
            row_lt['whatsapp_id'], idk_message, ["Audio_idk_raise", "Audio_idk_reask"], options, msg_id
        )
        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="query_response",
            message_category="IDK",
            message_id=sent_msg_id,
            audio_message_id=None,
            message_source_lang=idk_message,
            message_language=row_lt['user_language'],
            message_english=IDK,
            reply_id=msg_id,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=msg_id,
        )

    def handle_audio_idk_flow(self, msg_obj, row_lt):
        idk_options = self.template_messages["idk"][f"{row_lt['user_language']}"]["audio_options"]
        msg = msg_obj["interactive"]["button_reply"]["title"]
        msg_id = msg_obj["id"]
        bot_conv = self.bot_conv_db.get_from_message_id(msg_obj["context"]["id"])
        row_query = self.user_conv_db.get_from_message_id(bot_conv["reply_id"])

        previous_poll_response = bot_conv.get("poll_response", None)
        if previous_poll_response is not None:
            text = "You have already responded to this poll."
            text_src = self.azure_translate.translate_text(
                text, "en", row_lt['user_language'], self.logger
            )
            self.messenger.send_message(
                row_lt['whatsapp_id'], text_src, msg_id
            )
            return
        
        self.bot_conv_db.update_poll_response(
            bot_conv["_id"],
            msg_obj["interactive"]["button_reply"]["id"]
        )
        
        if msg in idk_options[0]:
            sent_msg_id, audio_message_id = self.send_idk_raise(row_lt, row_query, row_query["message_type"])
            query_type = row_query["query_type"]
            expert_type = self.category_to_expert[query_type]
            user_secondary_id = self.user_relation_db.find_user_relations(row_lt['user_id'], expert_type)['user_id_secondary']
            expert_row_lt = self.user_db.get_from_user_id(user_secondary_id)
            self.send_correction_poll_expert(row_lt, expert_row_lt, row_query)
        elif msg in idk_options[1]:
            msg = self.template_messages["idk"][f"{row_lt['user_language']}"]["audio_reask"]
            self.messenger.send_message(row_lt['whatsapp_id'], msg)

    def send_idk_raise(self, row_lt, row_query, msg_type="text"):
        raise_message = self.template_messages["idk"][row_lt['user_language']]["text"]
        expert_type = self.category_to_expert[row_query["query_type"]]
        expert_title = self.template_messages["expert_title"][row_lt['user_language']][expert_type]
        raise_message = raise_message.replace("<expert>", expert_title)
        _, list_title, questions_source, _ = self.get_related_questions(
            row_lt
        )
        sent_msg_id = self.messenger.send_suggestions(
            row_lt['whatsapp_id'], raise_message, list_title, questions_source, row_query['message_id']
        )
        audio_msg_id = None
        if msg_type == "audio":
            audio_output_file = "test_audio_output.ogg"
            self.azure_translate.text_to_speech(
                raise_message, row_lt['user_language']+'-IN', audio_output_file
            )
            audio_msg_id = self.messenger.send_audio(
                audio_output_file, row_lt['whatsapp_id'], row_query['message_id']
            )

        return sent_msg_id, audio_msg_id
        
    def handle_small_talk_idk(self, row_lt, row_query):
        text = self.template_messages["idk"]["out_of_scope_or_smalltalk_text"]
        final_message = self.template_messages["idk"][f"{row_lt['user_language']}"]["out_of_scope_or_smalltalk_text"]
        _, list_title, questions_source, _ = self.get_related_questions(row_lt)
        sent_msg_id = self.messenger.send_suggestions(
            row_lt['whatsapp_id'], final_message, list_title, questions_source, row_query['message_id']
        )
        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="query_response",
            message_id=sent_msg_id,
            audio_message_id=None,
            message_source_lang=final_message,
            message_language=row_lt['user_language'],
            message_english=text,
            reply_id=row_query["message_id"],
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=row_query["message_id"],
        )
        self.user_conv_db.mark_resolved(row_query["_id"])

    def send_query_response_and_follow_up(self, msg_type, msg_id, llm_response, row_lt, row_query):
        
        response = llm_response["response_en"]
        response_source = llm_response["response_src"]
        if msg_type == "audio":
            field = "new_question" if row_query["query_type"] != "small-talk" else "smalltalk"
            response = self.template_messages[field]["en"]["answer_audio"]
            response_source = self.template_messages[field][row_lt['user_language']]["answer_audio"]
            response = response.replace("<query>", row_query['message_english'])
            response_source = response_source.replace("<query>", row_query['message_source_lang'])
            response = response.replace("<answer>", llm_response["response_en"])
            response_source = response_source.replace("<answer>", llm_response["response_src"])

        # get related questions
        if response.startswith(IDK) or row_query["query_type"] == "small-talk":
            title, list_title, questions_source, questions_en = self.get_related_questions(row_lt)
        else:
            title, list_title = (
                self.onboarding_questions[row_lt["user_language"]]["title"],
                self.onboarding_questions[row_lt["user_language"]]["list_title"],
            )
            questions_en = llm_response["related_questions_en"]
            questions_source = llm_response["related_questions_src"]

            self.user_db.add_or_update_related_qns(
                row_lt['user_id'], questions_en)
            

        if len(questions_en) == 0:
            return self.send_query_response(msg_type, msg_id, response_source, row_lt)


        audio_msg_id = None
        
        sent_msg_id = self.messenger.send_suggestions(
            row_lt['whatsapp_id'], response_source, list_title, questions_source, msg_id
        )
        
        if msg_type == "audio":
            audio_output_file = "test_audio_output.ogg"
            self.azure_translate.text_to_speech(
                response_source, row_lt['user_language']+'-IN', audio_output_file
            )
            audio_msg_id = self.messenger.send_audio(
                audio_output_file, row_lt['whatsapp_id'], msg_id
            )
            utils.remove_extra_voice_files(None, audio_output_file)

        return sent_msg_id, audio_msg_id, response_source
    
    def send_preverified_response(self, msg_id, pre_verified_response, row_lt, row_query):

        if row_query['message_type'] == "audio":
            message_en = self.template_messages["previously_verified_answer"]["en"]["audio"]
            message_src = self.template_messages["previously_verified_answer"][row_lt['user_language']]["audio"]
            message_en = message_en.replace("<query>", row_query['message_english'])
            message_src = message_src.replace("<query>", row_query['message_source_lang'])
        else:
            message_en = self.template_messages["previously_verified_answer"]["en"]["text"]
            message_src = self.template_messages["previously_verified_answer"][row_lt['user_language']]["text"]

        similar_ques_en = pre_verified_response['question']
        similar_answer_en = pre_verified_response['metadata']['answer']

        similar_ques_src, similar_answer_src = self.azure_translate.translate_text_batch(
            [similar_ques_en, similar_answer_en], "en", row_lt['user_language']
        )

        message_en = message_en.replace("<similar_question>", similar_ques_en)
        message_src = message_src.replace("<similar_question>", similar_ques_src)
        message_en = message_en.replace("<similar_answer>", similar_answer_en)
        message_src = message_src.replace("<similar_answer>", similar_answer_src)
        
        options_src = self.template_messages["previously_verified_answer"][row_lt['user_language']]["options"]
        option_ids = ["PREVERIFIED_YES", "PREVERIFIED_NO"]
        sent_msg_id = self.messenger.send_message_with_options(
            row_lt['whatsapp_id'], message_src, option_ids, options_src, msg_id
        )
        new_conv = {"role": "assistant", "content": message_en}
        self.user_db.update_conv_history(
            row_lt['user_id'],
            new_conv,
            row_lt.get("conv_history", [])
        )

        row_lt['conv_history'] = row_lt.get("conv_history", []) + [new_conv]
        
        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="preverified_response",
            message_id=sent_msg_id,
            audio_message_id=None,
            message_source_lang=message_src,
            message_language=row_lt['user_language'],
            message_english=message_en,
            reply_id=msg_id,
            citations="preverified",
            message_timestamp=datetime.now(),
            transaction_message_id=msg_id,
            metadata={
                "preverified_id": pre_verified_response["id"],
                "related_chunk_ids": pre_verified_response["metadata"]["related_chunk_ids"],
            }
        )
        return


    def handle_preverified_response(self, msg_object, row_lt):
        msg_id = msg_object["id"]
        reply_id = msg_object["context"]["id"]
        bot_response = self.bot_conv_db.get_from_message_id(reply_id)
        row_query = self.user_conv_db.get_from_message_id(bot_response["transaction_message_id"])

        previous_poll_response = bot_response.get("poll_response", None)
        if previous_poll_response is not None:
            text = "You have already responded to this poll."
            text_src = self.azure_translate.translate_text(
                text, "en", row_lt['user_language'], self.logger
            )
            self.messenger.send_message(
                row_lt['whatsapp_id'], text_src, msg_id
            )
            return

        self.bot_conv_db.update_poll_response(
            bot_response["_id"],
            msg_object["interactive"]["button_reply"]["id"]
        )
        if msg_object["interactive"]["button_reply"]["id"] == "PREVERIFIED_YES":
            text = self.template_messages["previously_verified_answer"]["en"]["options_yes"]
            text_src = self.template_messages["previously_verified_answer"][row_lt['user_language']]["options_yes"]
            title, list_title, questions_source, questions_en = self.get_related_questions(
                row_lt, bot_response['metadata']['related_chunk_ids']
            )
            self.user_db.add_or_update_related_qns(
                row_lt['user_id'], questions_en)
            
            self.user_conv_db.mark_resolved(row_query["_id"])
            self.messenger.send_suggestions(
                row_lt['whatsapp_id'], text_src, list_title, questions_source, msg_id
            )
            self.user_conv_db.mark_resolved(row_query["_id"])
        elif msg_object["interactive"]["button_reply"]["id"] == "PREVERIFIED_NO":
            self.generate_and_send_response(row_query, row_lt)
        return
    
    def generate_and_send_response(self, row_query, row_lt):
        llm_response, citations, chunk_ids = self.knowledge_base.hierarchical_rag_answer_query(
            row_query, self.logger, row_lt
        )
        response = llm_response["response_en"]

        citations = "".join(citations)
        citations_str = citations
        query_type = row_query["query_type"]

        if response.strip().startswith(IDK):
            if query_type == "small-talk":
                self.handle_small_talk_idk(row_lt, row_query)
                return
            else:
                if row_query['message_type'] == "audio":
                    self.user_conv_db.add_query_type(
                    message_id=row_query['message_id'],
                    query_type=query_type
                    )
                    self.send_audio_idk_response(row_lt, row_query)
                else:
                    sent_msg_id, audio_msg_id = self.send_idk_raise(row_lt, row_query, row_query['message_type'])
                    raise_message = self.template_messages["idk"][row_lt['user_language']]["text"]
                    raise_message = raise_message.replace("<expert>", self.template_messages["expert_title"][row_lt['user_language']][self.category_to_expert[query_type]])
                    self.bot_conv_db.insert_row(
                    receiver_id=row_lt['user_id'],
                    message_type="query_response",
                    message_category="IDK",
                    message_id=sent_msg_id,
                    audio_message_id=audio_msg_id,
                    message_source_lang=raise_message,
                    message_language=row_lt['user_language'],
                    message_english=IDK,
                    reply_id=row_query['message_id'],
                    citations=None,
                    message_timestamp=datetime.now(),
                    transaction_message_id=row_query['message_id'],
                    )
                    expert_type = self.category_to_expert[query_type]
                    user_secondary_id = self.user_relation_db.find_user_relations(row_lt['user_id'], expert_type)['user_id_secondary']
                    expert_row_lt = self.user_db.get_from_user_id(user_secondary_id)
                    self.send_correction_poll_expert(row_lt, expert_row_lt, row_query)

                return

        if self.config['SUGGEST_NEXT_QUESTIONS']:
            sent_msg_id, audio_msg_id, response_source = self.send_query_response_and_follow_up(row_query['message_type'], row_query['message_id'], llm_response, row_lt, row_query)
        else:
            sent_msg_id, audio_msg_id, response_source = self.send_query_response(row_query['message_type'], row_query['message_id'], llm_response, row_lt)

        new_conv = {"role": "assistant", "content": response}
        self.user_db.update_conv_history(
            row_lt['user_id'],
            new_conv,
            row_lt.get("conv_history", [])
        )
        row_lt['conv_history'] = row_lt.get("conv_history", []) + [new_conv]

        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="query_response",
            message_id=sent_msg_id,
            audio_message_id=audio_msg_id,
            message_source_lang=response_source,
            message_language=row_lt['user_language'],
            message_english=response,
            reply_id=row_query['message_id'],
            citations=citations_str,
            message_timestamp=datetime.now(),
            transaction_message_id=row_query['message_id'],
            metadata={
                "retrieved_chunk_ids": chunk_ids,
            }
        )

        if (
            self.config["SEND_POLL"]
            and query_type != "small-talk"
        ):
            print("Sending poll and reaction")
            self.messenger.send_reaction(row_lt['whatsapp_id'], sent_msg_id, "\u2753")
            if row_query['message_type'] == "audio":
                self.messenger.send_reaction(row_lt['whatsapp_id'], audio_msg_id, "\u2753")
            expert_type = self.category_to_expert[query_type]
            user_secondary_id = self.user_relation_db.find_user_relations(row_lt['user_id'], expert_type)['user_id_secondary']
            expert_row_lt = self.user_db.get_from_user_id(user_secondary_id)
            self.send_correction_poll_expert(row_lt, expert_row_lt, row_query)

        return

    def answer_query_text(self, msg_id, row_query, row_lt):
        
        if self.config['PREVERIFIED'] and row_query['query_type'] != 'small-talk':
            pre_verified_response = self.preverified_client.find_closest_preverified_pair(
                query=row_query["message_context"],
                org_id=row_lt['org_id']
            )
            if pre_verified_response is not None:
                self.send_preverified_response(msg_id, pre_verified_response, row_lt, row_query)
                return

        self.generate_and_send_response(row_query, row_lt)
        return

    def get_related_questions(self, row_lt, chunk_ids=None):
        source_lang = row_lt["user_language"]
        if chunk_ids is not None:
            all_questions = []
            for chunk_id in chunk_ids:
                chunk = self.knowledge_base.kb_client.get_document(chunk_id)
                all_questions.extend((chunk["metadata"].get("related_questions", [])))
            random.shuffle(all_questions)
            questions_en = all_questions[:3]
        else:    
            questions_en = list(row_lt['related_qns'])
    
        questions_source = self.azure_translate.translate_text_batch(
            questions_en, "en", source_lang
        )
        title, list_title = (
            self.onboarding_questions[source_lang]["title"],
            self.onboarding_questions[source_lang]["list_title"],
        )

        return title, list_title, questions_source, questions_en

    def send_suggestions(self, row_lt, row_query, gpt_output):

        source_lang = row_lt["user_language"]
        
        title, list_title, questions_source, next_questions = self.get_related_questions(row_lt)
        
        suggested_ques_msg_id = self.messenger.send_suggestions(
            row_lt['whatsapp_id'], title, list_title, questions_source
        )

        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="suggested_questions",
            message_id=suggested_ques_msg_id,
            audio_message_id=None,
            message_source_lang=questions_source,
            message_language=source_lang,
            message_english=next_questions,
            reply_id=None,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=row_query['message_id'],
        )

    def handle_response_user(self, msg_object, row_lt):
        # data is a dictionary that contains from_number, msg_id, msg_object, user_type
        if self.check_expiration(row_lt):
            self.handle_expired_user_message(msg_object, row_lt)
            return
        msg_type = msg_object["type"]
        user_id = row_lt['user_id'] 
        msg_id = msg_object["id"]
        if (
            msg_object["type"] == "interactive"
            and msg_object["interactive"]["type"] == "list_reply"
            and msg_object["interactive"]["list_reply"]["id"][:5] == "LANG_"
        ):
            self.handle_language_poll_response(msg_object, row_lt)
            return
        if (
            msg_object["type"] == "interactive"
            and msg_object["interactive"]["type"] == "button_reply"
            and "Audio_idk" in msg_object["interactive"]["button_reply"]["id"]
        ):
            self.handle_audio_idk_flow(msg_object, row_lt)
            return
        
        if (
            msg_object["type"] == "interactive"
            and msg_object["interactive"]["type"] == "button_reply"
            and "PREVERIFIED" in msg_object["interactive"]["button_reply"]["id"]
        ):
            self.handle_preverified_response(msg_object, row_lt)
            return
        
        msg_text = None

        if msg_object.get("text") or (
            msg_object["type"] == "interactive"
            and msg_object["interactive"]["type"] == "list_reply"
            and msg_object["interactive"]["list_reply"]["id"][:5] == "QUEST"
        ):
            blob_name = None
            if msg_type == "interactive":
                msg_text = msg_object["interactive"]["list_reply"]["description"]
                self.logger.add_log(
                    sender_id=row_lt['whatsapp_id'],
                    receiver_id="bot",
                    message_id=msg_id,
                    action_type="click_suggestion",
                    details={"suggestion_text": msg_text},
                    timestamp=datetime.now(),
                )
            elif msg_type == "text":
                msg_text = msg_object["text"]["body"]
                self.logger.add_log(
                    sender_id=row_lt['whatsapp_id'],
                    receiver_id="bot",
                    message_id=msg_id,
                    action_type="send_message",
                    details={"text": msg_text, "reply_to": None},
                    timestamp=datetime.now(),
                )

        if msg_type == "audio":
            audio_input_file = "test_audio_input.ogg"
            audio_output_file = "test_audio_output.aac"
            utils.remove_extra_voice_files(audio_input_file, audio_output_file)
            self.messenger.download_audio(msg_object, audio_input_file)
            subprocess.call(
                ["ffmpeg", "-i", audio_input_file, audio_input_file[:-3] + "wav"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING").strip()
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_name = self.config["AZURE_BLOB_CONTAINER_NAME"].strip()

            blob_name = str(datetime.now()) + "_" + str(row_lt['whatsapp_id']) + ".ogg"
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )
            with open(file=audio_input_file, mode="rb") as data:
                blob_client.upload_blob(data)
            
            msg_text = self.azure_translate.speech_to_text(audio_input_file[:-3] + "wav", row_lt['user_language'], self.logger)
            self.logger.add_log(
                sender_id=row_lt['whatsapp_id'],
                receiver_id="bot",
                message_id=msg_id,
                action_type="send_message_audio",
                details={"message": msg_text, "reply_to": None},
                timestamp=datetime.now(),
            )
    

        conv_history = row_lt.get("conv_history", [])
        query_context = self.query_rewriter.translate_and_rewrite_query(msg_text, row_lt['user_language'], conv_history)
        new_conv = {"role": "user", "content": query_context["query_en"]}
        self.user_db.update_conv_history(
            row_lt['user_id'],
            new_conv,
            conv_history
        )
        row_lt['conv_history'] = conv_history + [new_conv]
        query_type = query_context["query_type"]
        
        db_id = self.user_conv_db.insert_user_query(
            user_id = row_lt['user_id'],
            message_id = msg_id,
            message_type = msg_type,
            message_source_lang = msg_text,
            source_language = row_lt['user_language'],
            message_english = query_context["query_en"],
            message_context= query_context["query_en_addcontext"],
            query_type= query_context["query_type"],
            audio_blob_path = None if msg_type != "audio" else blob_name,
            message_timestamp = datetime.now()
        ).inserted_id

        row_query = {
            '_id': db_id,
            'user_id': row_lt['user_id'],
            'message_id': msg_id,
            'message_type': msg_type,
            'message_source_lang': msg_text,
            'source_language': row_lt['user_language'],
            'message_english': query_context["query_en"],
            'message_context': query_context["query_en_addcontext"],
            'query_type': query_context["query_type"],
            'audio_blob_path': blob_name,
            'message_timestamp': datetime.now()
        }
        
        if query_type == "incomprehensible":
            self.handle_incomprehensible_query(msg_id, row_query, row_lt)
        elif query_type == "out_of_scope":
            self.handle_out_of_scope_query(msg_id, row_query, row_lt)
        else:    
            self.answer_query_text(msg_id, row_query, row_lt)
        return

    def handle_incomprehensible_query(self, msg_id, row_query, row_lt):
        if row_query['message_type'] == "audio":
            message_en = self.template_messages["idk"]["en"]["incomprehensible_audio"]
            message_src = self.template_messages["idk"][row_lt['user_language']]["incomprehensible_audio"]
            message_en = message_en.replace("<query>", row_query['message_english'])
            message_src = message_src.replace("<query>", row_query['message_source_lang'])
        else:
            message_en = self.template_messages["idk"]["en"]["incomprehensible_text"]
            message_src = self.template_messages["idk"][row_lt['user_language']]["incomprehensible_text"]
        questions_src = self.get_related_questions(row_lt)
        title, list_title, questions_src, questions_en = questions_src
        sent_msg_id = self.messenger.send_suggestions(
            row_lt['whatsapp_id'], message_src, list_title, questions_src, msg_id
        )
        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="query_response",
            message_id=sent_msg_id,
            audio_message_id=None,
            message_source_lang=message_src,
            message_language=row_lt['user_language'],
            message_english=message_en,
            reply_id=msg_id,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=msg_id,
        )
        return


    def handle_out_of_scope_query(self, msg_id, row_query, row_lt):
        if row_query['message_type'] == "audio":
            message_en = self.template_messages["idk"]["en"]["out_of_scope_or_smalltalk_audio"]
            message_src = self.template_messages["idk"][row_lt['user_language']]["out_of_scope_or_smalltalk_audio"]
            message_en = message_en.replace("<query>", row_query['message_english'])
            message_src = message_src.replace("<query>", row_query['message_source_lang'])
        else:
            message_en = self.template_messages["idk"]["en"]["out_of_scope_or_smalltalk_text"]
            message_src = self.template_messages["idk"][row_lt['user_language']]["out_of_scope_or_smalltalk_text"]
        
        title, list_title, questions_src, questions_en = self.get_related_questions(row_lt)
        sent_msg_id = self.messenger.send_suggestions(
            row_lt['whatsapp_id'], message_src, list_title, questions_src, msg_id
        )
        self.bot_conv_db.insert_row(
            receiver_id=row_lt['user_id'],
            message_type="query_response",
            message_id=sent_msg_id,
            audio_message_id=None,
            message_source_lang=message_src,
            message_language=row_lt['user_language'],
            message_english=message_en,
            reply_id=msg_id,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=msg_id,
        )
        return

    def handle_response_expert(self, msg_object, row_lt):
        msg_type = msg_object["type"]
        
        if (
            msg_type == "interactive"
            and msg_object["interactive"]["type"] == "button_reply"
            and msg_object["interactive"]["button_reply"]["id"][:12] == "POLL_PRIMARY"
        ) or (
            msg_type == "button"
        ):
            self.get_correction_poll_expert(msg_object, row_lt)
        elif msg_type == "text":
            self.get_correction_expert(msg_object, row_lt)


    def send_correction_poll_expert(self, row_lt, expert_row_lt, row_query, escalation=False, expert_row_lt_notif=None):
  
        row_bot_conv = self.bot_conv_db.find_with_transaction_id(row_query["message_id"], "query_response")

        if row_bot_conv is None:
            return
        
        user_type = row_lt["user_type"]
        
        poll_string = f"Was the bot's answer correct and complete?"

        citations = row_bot_conv["citations"]
        try:
            split_citations = citations.split("\n")
            split_citations = np.unique(
                np.array(
                    [
                        citation.replace("_", " ").replace("  ", " ").strip()
                        for citation in split_citations
                    ]
                )
            )
            final_citations = ", ".join([citation for citation in split_citations])
        except:
            final_citations = "No citations found."

        expert = self.category_to_expert[row_query['query_type']]
        
        receiver = expert_row_lt["whatsapp_id"]
        forward_to = expert
        try:
            patient_id = row_lt.get("patient_id", None)
            patient_id = f" {patient_id}" if patient_id is not None else ""
            gender = row_lt.get('patient_gender', None)
            gender = gender[0].upper() if gender is not None else "NA"
            surgery_date = row_lt.get("patient_surgery_date", None)
            surgery_date_str = f" *Surgery Date*: {surgery_date}" if surgery_date is not None else ""
            patient_details = f"*Patient*:{patient_id} {row_lt['patient_name']} {row_lt['patient_age']}/{gender}{surgery_date_str}"
        except:
            patient_details= ""

        # citation_str = f"*Citations*: {final_citations.strip()}. \n"
        if not utils.is_activity_older_than_24_hours(expert_row_lt.get("activity_timestamp", None)):
            poll_text = f'*Query*: "{row_query["message_english"]}" \n*Bot\'s Response*: {row_bot_conv["message_english"].strip()} \n{patient_details}\n{poll_string}'
            message_id = self.messenger.send_poll(
                    receiver, poll_text, poll_id="POLL_PRIMARY"
                )
        else:
            poll_text = f'*Query*: "{row_query["message_english"]}" \n*Bot\'s Response*: {row_bot_conv["message_english"].strip()} \n{patient_details}\n{poll_string}'
            message_id = self.messenger.send_template(
                receiver,
                "correction_poll",
                expert_row_lt["user_language"],
                [row_query["message_english"], row_bot_conv["message_english"].strip(), patient_details]
            )
        self.bot_conv_db.insert_row(
            receiver_id=expert_row_lt["user_id"],
            message_type=f"poll_{'escalated' if escalation else 'primary'}",
            message_id=message_id,
            audio_message_id=None,
            message_source_lang=poll_text,
            message_language=expert_row_lt["user_language"],
            message_english=poll_text,
            reply_id=None,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=row_query["message_id"],
        )

        if escalation:
            self.user_conv_db.mark_escalated(row_query["_id"])

            if expert_row_lt_notif is not None:
                primary_poll = self.bot_conv_db.find({"$and" : [{"receiver_id": expert_row_lt["user_id"]}, {"transaction_message_id": row_query["message_id"]}, {"message_type": "poll_primary"}]})
                receiver_name = f"escalation {expert}"
                primary_notif = expert_row_lt_notif["whatsapp_id"]
                self.messenger.send_message(
                    primary_notif,
                    "Escalating it to " + receiver_name,
                    reply_to_msg_id=primary_poll["message_id"],
                )
            
        return message_id


    def get_correction_poll_expert(self, msg_object, expert_row_lt):
        answer = msg_object["interactive"]["button_reply"]["title"] if msg_object["type"] == "interactive" else msg_object["button"]["payload"]
        context_id = msg_object["context"]["id"]

        self.logger.add_log(
            sender_id=msg_object["from"],
            receiver_id="bot",
            message_id=msg_object["id"],
            action_type="receive_poll",
            details={"answer": answer, "reply_to": context_id},
            timestamp=datetime.now(),
        )

        poll = self.bot_conv_db.get_from_message_id(context_id)

        if poll is None:
            self.messenger.send_message(
                msg_object["from"],
                self.template_messages["expert_verification"]["expert"]["en"]["notag"],
                msg_object["id"],
            )
            return
        
        transaction_message_id = poll["transaction_message_id"]
        row_query = self.user_conv_db.get_from_message_id(transaction_message_id)

        
        if expert_row_lt['user_type'] != self.category_to_expert[row_query['query_type']]:
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                f"This query has been forwarded to the {self.category_to_expert[row_query['query_type']]}.",
                context_id,
            )
            return

        if row_query.get("resolved", False):
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                "This query has already been answered.",
                context_id,
            )
            return

        row_response = self.bot_conv_db.find_with_transaction_id(transaction_message_id, "query_response")
        user_row_lt = self.user_db.get_from_user_id(row_query["user_id"])

        poll_responses = self.expert_conv_db.get_from_transaction_message_id(transaction_message_id, "poll_response")
        if len(poll_responses) > 0:
            poll_responses = sorted(poll_responses, key=lambda x: x['message_timestamp'])
            last_poll_response = poll_responses[-1]
            if last_poll_response['message'] == "No": # and last_poll_response["user_id"] != expert_row_lt["user_id"]:
                if last_poll_response["user_id"] == expert_row_lt["user_id"]:
                    self.messenger.send_message(
                        expert_row_lt['whatsapp_id'],
                        "You have already replied to this poll, please share the correction.",
                        context_id,
                    )
                else:
                    self.messenger.send_message(
                        expert_row_lt['whatsapp_id'],
                        "This query has already been answered.",
                        context_id,
                    )
                return


        if answer == "Yes":
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                self.template_messages["expert_verification"]["expert"]["en"]["query_yes"],
                context_id,
            )
            if row_response["message_category"] == "IDK":
                text = self.template_messages["idk"]["en"]["expertsaysyes"]
                text = text.replace("<phone_number>", self.unit_contact["phone_number"][user_row_lt["org_id"]])
                final_message = self.template_messages["idk"][f"{user_row_lt['user_language']}"]["expertsaysyes"]
                final_message = final_message.replace("<phone_number>", self.unit_contact["phone_number"][user_row_lt["org_id"]])
                
                sent_msg_id = self.messenger.send_message(
                    user_row_lt['whatsapp_id'], final_message, row_query["message_id"],
                )
            else:
                self.messenger.send_reaction(
                    user_row_lt['whatsapp_id'], row_response["message_id"], "\u2705"
                )
                if row_response["audio_message_id"]:
                    self.messenger.send_reaction(
                        user_row_lt['whatsapp_id'], row_response["audio_message_id"], "\u2705"
                    )
                text = self.template_messages["expert_verification"]["user"]["en"]["yes"]
                text = text.replace("<expert>", expert_row_lt["user_type"].lower())
                text_translated = self.template_messages["expert_verification"]["user"][f"{user_row_lt['user_language']}"]["yes"]
                expert_title = self.template_messages["expert_title"][user_row_lt["user_language"]][expert_row_lt['user_type']]
                text_translated = text_translated.replace("<expert>", expert_title)
                sent_msg_id = self.messenger.send_message(
                    user_row_lt["whatsapp_id"],
                    text_translated,
                    row_response["message_id"],
                )

                generalizable, question, response = self.preverified_client.anonymyze_qa_pair(row_query["message_english"], row_response["message_english"])
                
                if generalizable:
                    self.preverified_client.add_new_qa(
                        id=md5(question.encode('utf-8')).hexdigest(),
                        question=question,
                        answer=response,
                        related_chunk_ids=row_response.get("metadata", {}).get("retrieved_chunk_ids", []),
                        org_id=user_row_lt["org_id"],
                    )

            #Send green tick to the responding expert    
            self.messenger.send_reaction(
                expert_row_lt['whatsapp_id'], poll["message_id"], "\u2705"
            )

            #Send green tick to other expert (if any)
            if poll['message_type'] == "poll_primary":
                poll_notif = self.bot_conv_db.find_with_transaction_id(transaction_message_id, "poll_escalated")
            else:
                poll_notif = self.bot_conv_db.find_with_transaction_id(transaction_message_id, "poll_primary")

            if poll_notif is not None:
                notif_row_lt = self.user_db.get_from_user_id(poll_notif["receiver_id"])
                self.messenger.send_reaction(
                    notif_row_lt['whatsapp_id'], poll_notif["message_id"], "\u2705"
                )

            self.user_conv_db.mark_resolved(row_query['_id'])
            self.expert_conv_db.insert_row(
                user_id=expert_row_lt["user_id"],
                message_type="poll_response",
                message_id=msg_object["id"],
                reply_id=context_id,
                message="Yes",
                message_timestamp=datetime.now(),
                transaction_message_id=transaction_message_id,
            )

        elif answer == "No":
            if row_response["message_category"] == "IDK":
                pass
            else:
                self.messenger.send_reaction(
                    user_row_lt['whatsapp_id'], row_response["message_id"], "\u274C"
                )
                if row_response["audio_message_id"]:
                    self.messenger.send_reaction(
                        user_row_lt['whatsapp_id'], row_response["audio_message_id"], "\u274C"
                    )
                text = self.template_messages["expert_verification"]["user"]["en"]["no"]
                text = text.replace("<expert>", expert_row_lt["user_type"].lower())
                text_translated = self.template_messages["expert_verification"]["user"][f"{user_row_lt['user_language']}"]["no"]
                expert_title = self.template_messages["expert_title"][user_row_lt["user_language"]][expert_row_lt['user_type']]
                text_translated = text_translated.replace("<expert>", expert_title)
                self.messenger.send_message(
                    user_row_lt['whatsapp_id'],
                    text_translated,
                    row_response["message_id"],
                )
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                self.template_messages["expert_verification"]["expert"]["en"]["query_no"],
                context_id,
            )


            self.expert_conv_db.insert_row(
                user_id=expert_row_lt["user_id"],
                message_type="poll_response",
                message_id=msg_object["id"],
                reply_id=context_id,
                message="No",
                message_timestamp=datetime.now(),
                transaction_message_id=transaction_message_id,
            )


        return
        
    
    def get_correction_expert(self, msg_object, expert_row_lt):
        
        if msg_object.get("context", False) == False:
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                self.template_messages["expert_verification"]["expert"]["en"]["notag"],
                msg_object["id"],
            )
            return


        msg_body = msg_object["text"]["body"]
        context_id = msg_object["context"]["id"]

        self.logger.add_log(
            sender_id=msg_object["from"],
            receiver_id="bot",
            message_id=msg_object["id"],
            action_type="received_correction",
            details={"text": msg_body, "reply_to": context_id},
            timestamp=datetime.now(),
        )

        poll = self.bot_conv_db.get_from_message_id(context_id)

        if poll is not None and poll['message_type'] == 'consensus_poll':
            self.expert_conv_db.insert_row(
                user_id=expert_row_lt["user_id"],
                message_id=msg_object["id"],
                message_type="consensus_response",
                message=msg_body,
                reply_id=context_id,
                message_timestamp=datetime.now(),
                transaction_message_id=poll["transaction_message_id"],
            )
            self.messenger.send_message(
                msg_object["from"], self.template_messages["expert_verification"]["expert"]["en"]["query_no_correction"], msg_object["id"]
            )
            return
            
        # print("handling correction")
        if poll is None or (poll["message_type"] != "poll_primary" and poll["message_type"] != "poll_escalated"):
            self.messenger.send_message(
                msg_object["from"],
                self.template_messages["expert_verification"]["expert"]["en"]["notag"],
                msg_object["id"],
            )
            return
        
        transaction_message_id = poll["transaction_message_id"]

        row_query = self.user_conv_db.get_from_message_id(transaction_message_id)
        user_row_lt = self.user_db.get_from_user_id(row_query["user_id"])

        if row_query.get("resolved", False):
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                "This query has already been answered.",
                context_id,
            )
            return
        
        if expert_row_lt['user_type'] != self.category_to_expert[row_query['query_type']]:
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                f"This query has been forwarded to the {self.category_to_expert[row_query['query_type']]}.",
                context_id,
            )
            return
        
        row_response = self.bot_conv_db.find_with_transaction_id(transaction_message_id, "query_response")

        poll_responses = self.expert_conv_db.get_from_transaction_message_id(transaction_message_id, "poll_response")
        if len(poll_responses) == 0:
            self.messenger.send_message(
                expert_row_lt['whatsapp_id'],
                "Please reply to the poll of the query you are trying to fix before sending a correction.",
                context_id,
            )
            return
        else:
            poll_responses = sorted(poll_responses, key=lambda x: x['message_timestamp'])
            last_poll_response = poll_responses[-1]
            if last_poll_response['message'] != "No" and last_poll_response["user_id"] != expert_row_lt["user_id"]:
                self.messenger.send_message(
                    expert_row_lt['whatsapp_id'],
                    "This query has already been answered.",
                    context_id,
                )
                return

        db_id = self.expert_conv_db.insert_row(
            user_id=expert_row_lt["user_id"],
            message_id=msg_object["id"],
            message_type="correction",
            message=msg_body,
            reply_id=context_id,
            message_timestamp=datetime.now(),
            transaction_message_id=transaction_message_id,
        ).inserted_id

        row_correction = self.expert_conv_db.get_from_message_id(msg_object["id"])

            

        gpt_output = self.knowledge_base.generate_correction(row_query, row_response, row_correction, self.logger)
        gpt_output = gpt_output.strip('"')

        

        
        if row_query["message_type"] == "audio":
            corrected_audio_loc = "corrected_audio.ogg"
            remove_extra_voice_files(
                corrected_audio_loc, corrected_audio_loc[:-3] + ".aac"
            )
            verification_text = self.template_messages["expert_verification"]["user"]["en"]["yes"]
            verification_text = verification_text.replace("<expert>", expert_row_lt["user_type"].lower())
            verification_text_source = self.template_messages["expert_verification"]["user"][user_row_lt["user_language"]]["yes"]
            expert_title = self.template_messages["expert_title"][user_row_lt["user_language"]][expert_row_lt['user_type']]
            verification_text_source = verification_text_source.replace("<expert>", expert_title)
            gpt_output_source = self.azure_translate.translate_text(
                gpt_output, "en", user_row_lt['user_language'], self.logger
            )
            self.azure_translate.text_to_speech(
                gpt_output_source, user_row_lt['user_language'] + "-IN", corrected_audio_loc
            )

            gpt_output = f"{gpt_output}\n\n{verification_text}"
            gpt_output_source = f"{gpt_output_source}\n\n{verification_text_source}"

            updated_msg_id = self.messenger.send_message(
                user_row_lt['whatsapp_id'],
                gpt_output_source,
                row_query["message_id"],
            )
            updated_audio_msg_id = self.messenger.send_audio(
                corrected_audio_loc,
                user_row_lt['whatsapp_id'],
                row_query["message_id"]
            )
            remove_extra_voice_files(
                corrected_audio_loc, corrected_audio_loc[:-3] + ".aac"
            )

            self.messenger.send_reaction(user_row_lt['whatsapp_id'], updated_msg_id, "\u2705")
            self.messenger.send_reaction(
                user_row_lt['whatsapp_id'], updated_audio_msg_id, "\u2705"
            )
        else:
            verification_text = self.template_messages["expert_verification"]["user"]["en"]["yes"]
            verification_text = verification_text.replace("<expert>", expert_row_lt["user_type"].lower())
            verification_text_source = self.template_messages["expert_verification"]["user"][f"{user_row_lt['user_language']}"]["yes"]
            expert_title = self.template_messages["expert_title"][user_row_lt["user_language"]][expert_row_lt["user_type"]]
            verification_text_source = verification_text_source.replace("<expert>", expert_title)
            gpt_output_source = self.azure_translate.translate_text(
                gpt_output, "en", user_row_lt['user_language'], self.logger
            )
            gpt_output = f"{gpt_output}\n\n{verification_text}"
            gpt_output_source = f"{gpt_output_source}\n\n{verification_text_source}"
            updated_msg_id = self.messenger.send_message(
                user_row_lt['whatsapp_id'],
                gpt_output_source,
                row_query["message_id"],
            )
            updated_audio_msg_id = None
            self.messenger.send_reaction(user_row_lt['whatsapp_id'], updated_msg_id, "\u2705")

        new_conv = {"role": "assistant", "content": gpt_output}
        self.user_db.update_conv_history(
            user_row_lt['user_id'],
            new_conv,
            user_row_lt.get("conv_history", [])
        )
        user_row_lt['conv_history'] = user_row_lt.get("conv_history", []) + [new_conv]

        self.bot_conv_db.insert_row(
            receiver_id=user_row_lt['user_id'],
            message_type="query_correction",
            message_id=updated_msg_id,
            audio_message_id=updated_audio_msg_id,
            message_source_lang=gpt_output_source,
            message_language=user_row_lt['user_language'],
            message_english=gpt_output,
            reply_id=row_query["message_id"],
            citations="expert_correction",
            message_timestamp=datetime.now(),
            transaction_message_id=transaction_message_id
        )

        generalizable, question, response = self.preverified_client.anonymyze_qa_pair(row_query["message_english"], gpt_output)
        if generalizable:
            self.preverified_client.add_new_qa(
                id=md5(question.encode('utf-8')).hexdigest(),
                question=question,
                answer=response,
                related_chunk_ids=row_response.get("metadata", {}).get("retrieved_chunk_ids", []),
                org_id=user_row_lt["org_id"],
            )

        

        self.messenger.send_message(
            msg_object["from"], self.template_messages["expert_verification"]["expert"]["en"]["query_no_correction"], msg_object["id"]
        )
        self.user_conv_db.mark_resolved(row_query['_id'])
        
        
        if row_query['message_type'] == 'audio':
            remove_extra_voice_files(
                corrected_audio_loc, corrected_audio_loc[:-3] + ".aac"
            )
        return
    

    def send_query_expert(self, expert_row_lt, query_row):

        query = query_row["message_source_lang"]

        message = f"*Query*: {query}"

        message_id = self.messenger.send_message(
            expert_row_lt['whatsapp_id'], message, None
        )

        if query_row["message_type"] == "audio":
            audio_file = query_row["audio_blob_path"]
            
            
            connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING").strip()
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_name = self.config["AZURE_BLOB_CONTAINER_NAME"].strip()

            blob_client = blob_service_client.get_blob_client(container=container_name, blob=audio_file)
            download_file_path = "original_audio.ogg"
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            audio_msg_id = self.messenger.send_audio(audio_file, expert_row_lt['whatsapp_id'], message_id)

        else:
            audio_msg_id = None

        self.bot_conv_db.insert_row(
            receiver_id=expert_row_lt["user_id"],
            message_type="consensus_poll",
            message_id=message_id,
            audio_message_id=audio_msg_id,
            message_source_lang=query,
            message_language=query_row["source_language"],
            message_english=query,
            reply_id=None,
            citations=None,
            message_timestamp=datetime.now(),
            transaction_message_id=query_row["message_id"],
        )