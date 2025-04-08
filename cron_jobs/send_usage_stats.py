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

yes_responses = ["Yes", "हाँ।", "అవును.", "ஆம்.", "ಹೌದು.", "ہاں."]

def get_org_wise_stats(df, orgs, timestamp_col, variable=None):
    stats = []
    for org in orgs:
        org_df = df[df['org_id'] == org]
        if variable is not None:
            num_true = len(org_df[org_df[variable] == True])
            num_true_last_day = len(org_df[(org_df[variable] == True) & (org_df[timestamp_col] >= LAST_DAY)])
            stats.append(f"{num_true} ({num_true_last_day})")
        else:
            num = len(org_df)
            num_last_day = len(org_df[org_df[timestamp_col] >= LAST_DAY])
            stats.append(f"{num} ({num_last_day})")
    return stats

NUM_DAYS = 7
NUM_HOURS = NUM_DAYS*24
LAST_DAY = pd.to_datetime(datetime.datetime.now().date()) - pd.DateOffset(days=NUM_DAYS)


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

user_conv_cursor = user_conv_db.collection.find({})
user_conv_df = pd.DataFrame(list(user_conv_cursor))

user_conv_df['message_timestamp'] = pd.to_datetime(user_conv_df['message_timestamp'])
user_conv_df = user_conv_df[user_conv_df['message_timestamp'] >= CUT_OFF_START_DATE]

#retain if 'user_id' in users_df['user_id']


user_conv_df = user_conv_df[user_conv_df['user_id'].isin(users_df['user_id'])]

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


# EMAIL STAT
print(f"Number of people who said yes to onboarding: {onboarding_responses_df['is_yes'].sum()}")
print(f"Number of people who said no to onboarding: {len(onboarding_responses_df) - onboarding_responses_df['is_yes'].sum()}")

onboarding_yes_stats = ["Number of people who said yes to onboarding"] + get_org_wise_stats(onboarding_responses_df, orgs, 'message_timestamp', 'is_yes')
onboarding_no_stats = ["Number of people who said no to onboarding"] + get_org_wise_stats(onboarding_responses_df, orgs, 'message_timestamp', 'is_no')


user_conv_df_merged = user_conv_df.merge(users_df, left_on='user_id', right_on='user_id', how='inner')
users_df['num_messages_sent'] = users_df['user_id'].map(user_conv_df_merged['user_id'].value_counts())
user_conv_df_merged.rename(
    columns={
    'message_type': 'query_modality',
    'message_source_lang': 'query_source_lang',
    'message_english': 'query_english',
    'message_id': 'transaction_message_id',
}, inplace=True)

user_query_df = user_conv_df_merged[user_conv_df_merged['query_modality'].isin(['interactive', 'text', 'audio'])]


cursor = bot_conv_db.collection.find({})
bot_conv_df = pd.DataFrame(list(cursor))

query_responses_df = bot_conv_df[bot_conv_df['message_type'] == 'query_response']

query_responses_df.rename(
    columns={
    'message_source_lang': 'response_source_lang',
    'message_english': 'response_english'
}, inplace=True)

query_responses_df = query_responses_df[['transaction_message_id', 'response_source_lang', 'response_english']]
user_query_df = user_query_df.merge(query_responses_df, left_on='transaction_message_id', right_on='transaction_message_id', how='left')

cursor = expert_conv_db.collection.find({})
expert_conv_df = pd.DataFrame(list(cursor))

poll_response_df = expert_conv_df[expert_conv_df['message_type'] == 'poll_response']

poll_response_df.rename(
    columns={
    'message' : 'poll_response'
}, inplace=True)

poll_response_df = poll_response_df[['transaction_message_id', 'poll_response']]

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

# cols = ["patient_id", "patient_name", "query_modality", "query_source_lang", "query_english", "query_type", "response_source_lang", "response_english", "resolved", "poll_response", "correction", "correction_source_lang", "correction_english"]

unit_info = {
    "BLR": "Bangalore",
    "HYD": "Hyderabad",
    "JAI": "Jaipur",
}

stats = [["Description"] + [unit_info[org] for org in orgs]]

patient_stats = ["Number of patients"] + get_org_wise_stats(users_df, orgs, 'timestamp')
stats.append(patient_stats)

stats.append(onboarding_yes_stats)
stats.append(onboarding_no_stats)

query_df = user_query_df[user_query_df['query_type'] != 'small-talk']

query_stats = ["Number of queries"] + get_org_wise_stats(query_df, orgs, 'message_timestamp')
stats.append(query_stats)

query_resolved_stats = ["Number of queries resolved"] + get_org_wise_stats(query_df, orgs, 'message_timestamp', 'resolved')
stats.append(query_resolved_stats)

query_df['is_pending'] = query_df['resolved'].apply(lambda x: False if x == True else True)

query_pending_stats = ["Number of queries pending"] + get_org_wise_stats(query_df, orgs, 'message_timestamp', 'is_pending')
stats.append(query_pending_stats)

html_message = f"""
<html>
<head>
    <style>
    table {{
        border-collapse: collapse;
        width: 100%;
    }}
    th, td {{
        border: 1px solid black;
        padding: 10px;  /* Increase padding to increase space between columns */
        text-align: left;
    }}
</style>
</head>
    <body style="font-family: 'Courier New', Courier, monospace;">
    Hello team,<br>
    Please find the usage stats for CataractBot below:<br>
    The values in the bracket indicate the change in the last {NUM_DAYS} day(s).<br>
    <h4>Overall Stats:</h4>
    {tabulate(stats, tablefmt="html", colalign=("left", "left"))}
    <br>
    Regards,<br>
    CataractBot Team
</body>
</html>
"""

print(html_message)

s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
s.login(os.environ['LOGGING_EMAIL_ID'].strip(), os.environ['LOGGING_EMAIL_PASS'].strip())

date_today = pd.to_datetime(datetime.datetime.now()).strftime("%Y-%m-%d")

credential = DefaultAzureCredential()
client = SecretClient(vault_url=os.environ['AZ_KEY_VAULT_URL'].strip(), credential=credential)
secret = client.get_secret("logging-email-list")
email_list = eval(secret.value)

msg = MIMEMultipart('alternative')
msg['Subject'] = f"CataractBot usage stats for {date_today}"
msg['From'] = os.environ['LOGGING_EMAIL_ID'].strip()
msg['To'] = ", ".join(email_list)

# Attach the HTML message
msg.attach(MIMEText(html_message, 'html'))


#save the email to an html file
with open('email.html', 'w') as file:
    file.write(msg.as_string())

print("Sending email to: ", email_list)
for receiver in email_list:
    s.sendmail(os.environ['LOGGING_EMAIL_ID'].strip(), receiver, msg.as_string())
s.quit()