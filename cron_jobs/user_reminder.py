
import yaml
import os

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")

from app_logging import LoggingDatabase
from messenger.whatsapp import WhatsappMessenger
from database import UserDB
from database import UserConvDB
import os
import json
import datetime
from tqdm import tqdm

template_name = "reminder_cataractbot"


logger = LoggingDatabase(config)
messenger = WhatsappMessenger(config, logger)

user_db = UserDB(config)
user_conv_db = UserConvDB(config)

all_patients = user_db.get_all_patients()

total_reminders = 0

for patient in tqdm(all_patients, total=len(all_patients), desc="Processing patients"):
    user_id = patient["user_id"]
    whatsapp_id = patient["whatsapp_id"]
    user_language = patient["user_language"]

    if not patient.get("onboarded", False):
        continue

    onboarding_ts = patient.get("timestamp", None)

    send_reminder = False
    reminder_type = None

    if onboarding_ts:
        # Convert timestamp to datetime if it's a string
        if isinstance(onboarding_ts, str):
            try:
                onboarding_datetime = datetime.datetime.fromisoformat(onboarding_ts.replace('Z', '+00:00'))
            except ValueError:
                # Try different format if isoformat fails
                try:
                    onboarding_datetime = datetime.datetime.strptime(onboarding_ts, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print(f"Could not parse timestamp {onboarding_ts} for user {user_id}")
                    continue
        else:
            onboarding_datetime = onboarding_ts
        
        # Calculate days since onboarding
        days_since_onboarding = (datetime.datetime.now() - onboarding_datetime).days
        
        # Send reminder if it's been 4 days since onboarding
        if days_since_onboarding == 4:
            send_reminder = True
            reminder_type = "onboarding_reminder"

    patient_surgery_date = patient.get("patient_surgery_date", None)
    if patient_surgery_date:
        # Convert timestamp to datetime if it's a string
        if isinstance(patient_surgery_date, str):
            try:
                surgery_datetime = datetime.datetime.fromisoformat(patient_surgery_date.replace('Z', '+00:00'))
            except ValueError:
                # Try different format if isoformat fails
                try:
                    surgery_datetime = datetime.datetime.strptime(patient_surgery_date, "%d-%m-%Y")
                except:
                    print(f"Could not parse surgery date {patient_surgery_date} for user {user_id}")
                    continue
        else:
            surgery_datetime = patient_surgery_date
        
        # Calculate days relative to surgery
        current_date = datetime.datetime.now()
        days_to_surgery = (surgery_datetime.date() - current_date.date()).days
        days_after_surgery = (current_date.date() - surgery_datetime.date()).days

        # Send reminder if it's 1 day before surgery
        if days_to_surgery == 1:
            send_reminder = True
            reminder_type = "pre_surgery_day"
        # Send reminder if it's 1 day after surgery
        elif days_after_surgery == 1:
            send_reminder = True
            reminder_type = "post_surgery_day"
        # Send reminder if it's 1 week (7 days) after surgery
        elif days_after_surgery == 7:
            send_reminder = True
            reminder_type = "post_surgery_week"

        
    if send_reminder:
        print(f"Sending reminder to {whatsapp_id} for {reminder_type}")
        sent_msg_id = messenger.send_template(
            whatsapp_id,
            template_name,
            user_language
        )

        logger.add_log(
            sender_id="bot",
            receiver_id=whatsapp_id,
            message_id=sent_msg_id,
            action_type="send_reminder",
            details={"reminder_type": reminder_type},
            timestamp=datetime.datetime.now(),
        )
        total_reminders += 1

print(f"Total reminders sent: {total_reminders}")