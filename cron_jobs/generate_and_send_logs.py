import pytz
import yaml
import os
import smtplib
local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys
sys.path.append(local_path + "/src")

from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB
from messenger.whatsapp import WhatsappMessenger
from tabulate import tabulate
import datetime
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import utils
from send_usage_stats import send_email_with_stats

yes_responses = ["Yes", "हाँ।", "అవును.", "ஆம்.", "ಹೌದು.", "ہاں."]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.environ['SPREADSHEET_ID'].strip()

IST = pytz.timezone('Asia/Kolkata')
DT_NOW = pd.to_datetime(datetime.datetime.now(IST).date())
CUT_OFF_START_DATE = pd.to_datetime('2024-12-20')


user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)

users = user_db.collection.find({})
users_df = pd.DataFrame(users)

orgs = ["BLR", "HYD", "JAI"]

users_df = users_df[users_df['org_id'].isin(orgs)]


counsellors_df = users_df[users_df['user_type'] == 'Counsellor']
doctors_df = users_df[users_df['user_type'] == 'Doctor']
users_df = users_df[users_df['user_type'] == 'Patient']
users_df = users_df[(users_df['timestamp'] >= CUT_OFF_START_DATE) & (users_df['timestamp'] <= DT_NOW)]

experts_df = pd.concat([counsellors_df, doctors_df], ignore_index=True)

user_conv_cursor = user_conv_db.collection.find({})
user_conv_df = pd.DataFrame(list(user_conv_cursor))

cursor = bot_conv_db.collection.find({})
bot_conv_df = pd.DataFrame(list(cursor))

user_conv_df['message_timestamp'] = pd.to_datetime(user_conv_df['message_timestamp'])
user_conv_df = user_conv_df[(user_conv_df['message_timestamp'] >= CUT_OFF_START_DATE) & (user_conv_df['message_timestamp'] <= DT_NOW)]

bot_conv_df['message_timestamp'] = pd.to_datetime(bot_conv_df['message_timestamp'])
bot_conv_df = bot_conv_df[(bot_conv_df['message_timestamp'] >= CUT_OFF_START_DATE) & (bot_conv_df['message_timestamp'] <= DT_NOW)]

user_conv_df = user_conv_df[user_conv_df['user_id'].isin(users_df['user_id'])]

onboarding_messages_df = bot_conv_df[bot_conv_df['message_type'] == 'onboarding_template']

#drop duplicate receiver_id, keep last
onboarding_messages_df = onboarding_messages_df.sort_values(by='message_timestamp', ascending=False)
onboarding_messages_df = onboarding_messages_df.drop_duplicates(subset=['receiver_id'], keep='first')

onboarding_messages_df = onboarding_messages_df.merge(users_df, left_on='receiver_id', right_on='user_id', how='inner')

users_df = users_df.merge(onboarding_messages_df[['receiver_id', 'message_status']], left_on='user_id', right_on='receiver_id', how='left')

onboarding_responses_df = user_conv_df[user_conv_df['message_type'] == 'onboarding_response']
lang_poll_responses_df = user_conv_df[user_conv_df['message_type'] == 'lang_poll_response']

#join with patients_df on user_id and find how many patients have completed onboarding and lang poll
onboarding_responses_df = onboarding_responses_df.merge(users_df, left_on='user_id', right_on='user_id', how='inner')
lang_poll_responses_df = lang_poll_responses_df.merge(users_df, left_on='user_id', right_on='user_id', how='inner')

#if there is no user, drop the row
onboarding_responses_df = onboarding_responses_df.dropna(subset=['user_id'])
lang_poll_responses_df = lang_poll_responses_df.dropna(subset=['user_id'])

#drop duplicates
onboarding_responses_df = onboarding_responses_df.drop_duplicates(subset=['user_id'], keep='last')
lang_poll_responses_df = lang_poll_responses_df.drop_duplicates(subset=['user_id'], keep='last')

onboarding_responses_df['is_yes'] = onboarding_responses_df['message_source_lang'].apply(lambda x: x in yes_responses)
onboarding_responses_df['is_no'] = onboarding_responses_df['message_source_lang'].apply(lambda x: x not in yes_responses)

#assert that the sum of is_yes and is_no is equal to the number of rows
assert onboarding_responses_df['is_yes'].sum() + onboarding_responses_df['is_no'].sum() == len(onboarding_responses_df)

users_df = users_df.merge(onboarding_responses_df[['user_id', 'is_yes', 'is_no']], left_on='user_id', right_on='user_id', how='left')


users_df['onboarding_message_status'] = users_df['message_status']
users_df['onboarding_response'] = users_df.apply(lambda row: 'Yes' if row['is_yes'] == True else ('No' if row['is_no'] == True else ''), axis=1)

user_conv_df_merged = user_conv_df.merge(users_df, left_on='user_id', right_on='user_id', how='inner')
users_df['num_messages_sent'] = users_df['user_id'].map(user_conv_df_merged['user_id'].value_counts())
user_conv_df_merged.rename(
    columns={
    'message_type': 'query_modality',
    'message_source_lang': 'query_source_lang',
    'message_english': 'query_english',
    'message_timestamp': 'query_timestamp',
    'message_id': 'transaction_message_id',
}, inplace=True)

user_query_df = user_conv_df_merged[user_conv_df_merged['query_modality'].isin(['interactive', 'text', 'audio'])]


query_responses_df = bot_conv_df[bot_conv_df['message_type'] == 'query_response']
preverified_responses_df = bot_conv_df[bot_conv_df['message_type'] == 'preverified_response']
preverified_responses_df['preverified_response'] = True

preverified_responses_df.rename(
    columns={
    'message_source_lang': 'preverified_response_source_lang',
    'message_english': 'preverified_response_english',
    'poll_response': 'preverified_poll_response'
}, inplace=True)

preverified_responses_df = preverified_responses_df[['transaction_message_id', 'preverified_response', 'preverified_response_source_lang', 'preverified_response_english', 'preverified_poll_response']]
user_query_df = user_query_df.merge(preverified_responses_df, left_on='transaction_message_id', right_on='transaction_message_id', how='left')

def apply_preverified_response(row):
    # Already resolved or has valid preverified response
    return (row.get('resolved') == True or 
           (row.get('preverified_response') == True and 
            row.get('preverified_poll_response') != "PREVERIFIED_NO"))

user_query_df['resolved'] = user_query_df.apply(apply_preverified_response, axis=1)

query_responses_df.rename(
    columns={
    'message_source_lang': 'response_source_lang',
    'message_english': 'response_english'
}, inplace=True)

query_responses_df = query_responses_df[['transaction_message_id', 'response_source_lang', 'response_english']]
user_query_df = user_query_df.merge(query_responses_df, left_on='transaction_message_id', right_on='transaction_message_id', how='left')

# Check how many queries have a non-null preverified response
queries_with_preverified = user_query_df[user_query_df['preverified_response_source_lang'].notna()]


cursor = expert_conv_db.collection.find({})
expert_conv_df = pd.DataFrame(list(cursor))

poll_response_df = expert_conv_df[expert_conv_df['message_type'] == 'poll_response']

poll_response_df = poll_response_df.merge(experts_df, left_on='user_id', right_on='user_id', how='inner')

poll_response_df.rename(
    columns={
    'user_name': 'expert_name',
    'message' : 'poll_response'
}, inplace=True)


poll_response_df = poll_response_df[['transaction_message_id', 'poll_response', 'expert_name']]

if len(poll_response_df) > 0:
    #join with user query df, left on message_id and right on transaction_message_id
    user_query_df = user_query_df.merge(poll_response_df, left_on='transaction_message_id', right_on='transaction_message_id', how='left')

correction_df = expert_conv_df[expert_conv_df['message_type'] == 'correction']

correction_df.rename(
    columns={
    'message' : 'correction'
}, inplace=True)

correction_df = correction_df[['transaction_message_id', 'correction']]

if len(correction_df) > 0:
    #join with user query df, left on message_id and right on transaction_message_id
    user_query_df = user_query_df.merge(correction_df, left_on='transaction_message_id', right_on='transaction_message_id', how='left')

query_correction_df = bot_conv_df[bot_conv_df['message_type'] == 'query_correction']

query_correction_df.rename(
    columns={
    'message_source_lang': 'correction_source_lang',
    'message_english': 'correction_english'
}, inplace=True)

query_correction_df = query_correction_df[['transaction_message_id', 'correction_source_lang', 'correction_english']]

if len(query_correction_df) > 0:
    #join with user query df, left on message_id and right on transaction_message_id
    user_query_df = user_query_df.merge(query_correction_df, left_on='transaction_message_id', right_on='transaction_message_id', how='left')

user_query_df['resolved'].fillna(False, inplace=True)
user_query_df['resolved'] = user_query_df['resolved'].astype(bool)

user_query_df.loc[user_query_df['query_type'].isin(['small-talk', 'expired_access']), 'resolved'] = True


user_query_df['pending'] = user_query_df['resolved'].apply(lambda x: "" if x == True else "Yes")


cols = ["patient_id", "patient_surgery_date", "user_language", "query_timestamp", "query_modality", "query_source_lang", "query_english", "query_type", 'preverified_response_source_lang', 'preverified_response_english', 'preverified_poll_response', "response_source_lang", "response_english", 'expert_name', "poll_response", "correction", "correction_source_lang", "correction_english", "org_id", "pending"]

# change lang mapping
lang_mapping = {
    "en": "English",
    "hi": "Hindi",
    "te": "Telugu",
    "ta": "Tamil",
    "kn": "Kannada",
    "ur": "Urdu",
}

logs_df = user_query_df[cols].copy()

logs_df['user_language'] = logs_df['user_language'].map(lang_mapping)

#sort from latest to oldest
logs_df.sort_values(by='query_timestamp', ascending=False, inplace=True)
logs_df.reset_index(drop=True, inplace=True)


logs_df['patient_surgery_date'] = pd.to_datetime(logs_df['patient_surgery_date'], errors='coerce')
logs_df['patient_surgery_date'] = logs_df['patient_surgery_date'].dt.strftime('%d-%m-%Y')

# Convert query_timestamp to datetime and format it
logs_df['query_timestamp'] = pd.to_datetime(logs_df['query_timestamp'], errors='coerce')

# Retain last 6 months of data
six_months_ago = pd.to_datetime('now') - pd.DateOffset(months=6)
logs_df = logs_df[logs_df['query_timestamp'] >= six_months_ago]

logs_df['query_timestamp'] = logs_df['query_timestamp'].dt.strftime('%I:%M %p %d-%m-%Y')

logs_df.fillna('', inplace=True)
logs_df = logs_df.astype(str)

#rename all selected columns appropriately
logs_df.rename(
    columns={
        'patient_id': 'Patient ID',
        'patient_surgery_date': 'Patient Surgery Date',
        'user_language': 'Patient Language',
        'query_timestamp': 'Query Timestamp',
        'query_modality': 'Query Modality',
        'query_source_lang': 'Query in Source Language',
        'query_english': 'Query in English',
        'query_type': 'Query Type',
        'preverified_response_source_lang': 'Preverified Response in Source Language',
        'preverified_response_english': 'Preverified Response in English',
        'preverified_poll_response': 'Preverified Poll Response',
        'response_source_lang': 'Response in Source Language',
        'response_english': 'Response in English',
        'pending': 'Pending',
        'expert_name': 'Expert Name',
        'poll_response': 'Expert Poll Response',
        'correction': 'Correction',
        'correction_source_lang': 'Final Response in Source Language',
        'correction_english': 'Final Response in English'}
, inplace=True)

# Replace "PREVERIFIED_YES" and "PREVERIFIED_NO" with "Yes" and "No" in the Preverified Poll Response column
if 'Preverified Poll Response' in logs_df.columns:
    logs_df['Preverified Poll Response'] = logs_df['Preverified Poll Response'].replace({
        "PREVERIFIED_YES": "Yes",
        "PREVERIFIED_NO": "No"
    })

unit_info = {
    "BLR": "Bangalore",
    "HYD": "Hyderabad",
    "JAI": "Jaipur",
}

users_df.sort_values(by='timestamp', ascending=False, inplace=True)
users_df.reset_index(drop=True, inplace=True)
users_df_to_log = users_df.copy()
users_df_to_log.rename(
    columns={
        # 'user_id': 'User ID',
        'patient_id': 'Patient ID',
        'timestamp': 'Patient Onboarding Date',
        'patient_name': 'Patient Name',
        'patient_age': 'Patient Age',
        'patient_gender': 'Patient Gender',
        'patient_surgery_date': 'Patient Surgery Date',
        'onboarding_message_status': 'Onboarding Message Status',
        'onboarding_response': 'Onboarding Response',
        'org_id': 'Unit',
        'onboarding_message_status': 'Onboarding Message Status',
        'onboarding_response': 'Onboarding Response',
        'num_messages_sent': 'Number of Messages Sent',
    }, inplace=True
)

users_df_to_log = users_df_to_log[['Patient ID', 'Patient Onboarding Date', 'Patient Name', 'Patient Age', 'Patient Gender', 'Patient Surgery Date', 'Onboarding Message Status', 'Onboarding Response', 'Unit', 'Number of Messages Sent']].copy()
users_df_to_log['Unit'] = users_df_to_log['Unit'].map(unit_info)
users_df_to_log['Patient Surgery Date'] = pd.to_datetime(users_df_to_log['Patient Surgery Date'], errors='coerce').dt.strftime('%d-%m-%Y')

#only retain last 6 months of data
users_df_to_log = users_df_to_log[users_df_to_log['Patient Onboarding Date'] >= six_months_ago]
users_df_to_log['Patient Onboarding Date'] = pd.to_datetime(users_df_to_log['Patient Onboarding Date'], errors='coerce').dt.strftime('%d-%m-%Y')


users_df_to_log.fillna('', inplace=True)

users_df_to_log = users_df_to_log.head(2000)

utils.overwrite_sheet_data(SCOPES, SPREADSHEET_ID, 'Patients', users_df_to_log, local_path)

for org in orgs:
    logs_df_org = logs_df[logs_df['org_id'] == org]
    logs_df_org = logs_df_org.drop(columns=['org_id'])

    logs_df_org = logs_df_org.head(5000)
    
    utils.overwrite_sheet_data(SCOPES, SPREADSHEET_ID, org, logs_df_org, local_path)


send_email_with_stats(users_df, user_query_df, onboarding_messages_df, onboarding_responses_df, lang_poll_responses_df)