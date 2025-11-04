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

DT_NOW = datetime.datetime.now()
NUM_DAYS = 7
NUM_HOURS = NUM_DAYS*24
LAST_DAY = pd.to_datetime(DT_NOW) - pd.DateOffset(days=1)
LAST_DELTA_DAYS = pd.to_datetime(DT_NOW) - pd.DateOffset(days=NUM_DAYS)

def get_org_wise_stats(df, orgs, timestamp_col, variable=None):
    stats = []
    for org in orgs:
        org_df = df[df['org_id'] == org]
        if variable is not None:
            num_true = len(org_df[org_df[variable] == True])
            num_true_last_delta_day = len(org_df[(org_df[variable] == True) & (org_df[timestamp_col] >= LAST_DELTA_DAYS)])
            num_true_last_day = len(org_df[(org_df[variable] == True) & (org_df[timestamp_col] >= LAST_DAY)])
            stats.append(f"{num_true} ({num_true_last_delta_day}) [{num_true_last_day}]")
        else:
            num = len(org_df)
            num_last_delta_day = len(org_df[org_df[timestamp_col] >= LAST_DELTA_DAYS])
            num_last_day = len(org_df[org_df[timestamp_col] >= LAST_DAY])
            stats.append(f"{num} ({num_last_delta_day}) [{num_last_day}]")
    return stats


SPREADSHEET_ID = os.environ['SPREADSHEET_ID'].strip()

orgs = ["BLR", "HYD", "JAI"]

unit_info = {
    "BLR": "Bangalore",
    "HYD": "Hyderabad",
    "JAI": "Jaipur",
}

def send_email_with_stats(users_df, user_query_df, onboarding_messages_df, onboarding_responses_df, lang_poll_responses_df):

    onboarding_responses_df = onboarding_responses_df.dropna(subset=['user_id'])
    lang_poll_responses_df = lang_poll_responses_df.dropna(subset=['user_id'])

    onboarding_messages_df['is_sent'] = onboarding_messages_df['message_status'].apply(lambda x: True if x in ['sent', 'delivered', 'read'] else False)
    onboarding_messages_df['is_delivered'] = onboarding_messages_df['message_status'].apply(lambda x: True if x in ['delivered', 'read'] else False)
    onboarding_messages_df['is_read'] = onboarding_messages_df['message_status'].apply(lambda x: True if x == 'read' else False)

    onboarding_sent_stats = ["Number of onboarding messages sent"] + get_org_wise_stats(onboarding_messages_df, orgs, 'message_timestamp', 'is_sent')
    onboarding_delivered_stats = ["Number of onboarding messages delivered"] + get_org_wise_stats(onboarding_messages_df, orgs, 'message_timestamp', 'is_delivered')
    onboarding_read_stats = ["Number of onboarding messages read"] + get_org_wise_stats(onboarding_messages_df, orgs, 'message_timestamp', 'is_read')
    onboarding_yes_stats = ["Number of people who said yes to onboarding"] + get_org_wise_stats(onboarding_responses_df, orgs, 'message_timestamp', 'is_yes')
    onboarding_no_stats = ["Number of people who said no to onboarding"] + get_org_wise_stats(onboarding_responses_df, orgs, 'message_timestamp', 'is_no')

    stats = [["Description"] + [unit_info[org] for org in orgs]]

    patient_stats = ["Number of patients"] + get_org_wise_stats(users_df, orgs, 'timestamp')
    stats.append(patient_stats)
    stats.append(onboarding_sent_stats)
    stats.append(onboarding_delivered_stats)
    stats.append(onboarding_read_stats)
    stats.append(onboarding_yes_stats)
    stats.append(onboarding_no_stats)

    query_df = user_query_df[user_query_df['query_type'] != 'small-talk']

    query_stats = ["Number of queries"] + get_org_wise_stats(query_df, orgs, 'query_timestamp')
    stats.append(query_stats)

    query_resolved_stats = ["Number of queries resolved"] + get_org_wise_stats(query_df, orgs, 'query_timestamp', 'resolved')
    stats.append(query_resolved_stats)

    query_df['pending'] = query_df['pending'].apply(lambda x: True if x == "Yes" else False)

    stats.append(["Number of preverified responses sent"] + get_org_wise_stats(query_df, orgs, 'query_timestamp', 'preverified_response'))

    query_df['preverified_poll_response_yes'] = query_df['preverified_poll_response'].apply(lambda x: True if x == "PREVERIFIED_YES" else False)
    query_df['preverified_poll_response_no'] = query_df['preverified_poll_response'].apply(lambda x: True if x == "PREVERIFIED_NO" else False)

    stats.append(["Number of patients who found preverified responses helpful"] + get_org_wise_stats(query_df, orgs, 'query_timestamp', 'preverified_poll_response_yes'))
    stats.append(["Number of patients who found preverified responses not helpful"] + get_org_wise_stats(query_df, orgs, 'query_timestamp', 'preverified_poll_response_no'))
    
    query_pending_stats = ["Number of queries pending"] + get_org_wise_stats(query_df, orgs, 'query_timestamp', 'pending')
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
            padding: 10px;
            text-align: left;
        }}
        </style>
    </head>
    <body style="font-family: 'Courier New', Courier, monospace;">
        Hello team,<br>
        Here is the link to the usage logs:
        <a href="https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/view?gid=1306836655#gid=1306836655">Usage Logs</a>
        <br><br>
        Please find the usage stats for CataractBot below.<br>
        <small>
            Values in parentheses show the change over the last {NUM_DAYS} day(s), and values in square brackets show the change in the last 24 hours.
        </small>
        <h4>Overall Stats:</h4>
        {tabulate(stats, tablefmt="html", colalign=('left', 'left'))}
        <br>
        Regards,<br>
        CataractBot Team
    </body>
    </html>
    """

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

    for receiver in email_list:
        s.sendmail(os.environ['LOGGING_EMAIL_ID'].strip(), receiver, msg.as_string())
    s.quit()