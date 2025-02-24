import yaml
import os
import traceback
local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")
from datetime import datetime
from conversation_database import LoggingDatabase
from messenger.whatsapp import WhatsappMessenger
import os
from medics_integration import OnboardMedics
from az_table import PatientTable, DoctorAlternateTable
from database import UserDB, UserConvDB, ExpertConvDB, BotConvDB, UserRelationDB

logger = LoggingDatabase(config)

patient_table = PatientTable()
doctor_table = DoctorAlternateTable()
medics_onboard = OnboardMedics(config, logger)
user_db = UserDB(config)

entities = patient_table.fetch_all_rows()

doctor_alternate_entities = doctor_table.fetch_all_rows()

# doctors_for_onboarding = 

import pandas as pd

df = pd.DataFrame(entities)

timestamps = {}
for entity in entities:
    timestamps[entity['RowKey']] = entity._metadata['timestamp']



# #add to hyd_df if HYD in MRD
# hyd_df = df[df['MRD'].str.contains('HYD')]

# print(hyd_df['operating_doctor'].value_counts())
# print(hyd_df['counsellor_name'].value_counts())
# df = df[df['surgery_name'] == 'CATARACT']
# df['unit'] = df['MRD'].apply(lambda x: x.split('/')[0])
# doctor_df = df[['operating_doctor', 'operating_doctor_number', 'unit']]

# doctor_df = doctor_df.drop_duplicates()

# # doctor_df.to_excel('/mnt/c/Users/b-bsachdeva/Documents/doctors.xlsx', index=False)

# print(hyd_df.head())  

# df = df[df['MRD'].str.contains('JAI')]
# print(len(df))

# print(df['operating_doctor'].value_counts())

#print all doctors and their phone numbers
# for doc in df['operating_doctor'].unique():
#     print(doc, df[df['operating_doctor'] == doc]['operating_doctor_number'].iloc[0])



cutoff_date = pd.to_datetime(datetime.now().date())
df['surgery_date'] = pd.to_datetime(df['surgery_date'], errors='coerce')
df['surgery_date'] = df['surgery_date'].apply(lambda x: x.date())
df['surgery_date'] = pd.to_datetime(df['surgery_date'])
df['surgery_date'] = df['surgery_date'].apply(lambda x: x + pd.DateOffset(days=1))

# df = df[df['surgery_date'] >= cutoff_date]

df['ts'] = df['RowKey'].apply(lambda x: timestamps[x])
df['ts'] = pd.to_datetime(df['ts'])


# print the latest date in the df
# print(df['ts'].max())

#sort by ts
df = df.sort_values(by='ts', ascending=False)

# print(df.dtypes)
# df['ts'] = pd.to_datetime(df['ts'], utc=True)
# df['ts'] = df['ts'].dt.tz_convert(None)  # Convert to naive datetime
# df = df[df['ts'] > (pd.to_datetime(datetime.now()) - pd.DateOffset(weeks=2))]

print(df.columns)

blr_df = df[df['MRD'].str.contains('BLR')]
hyd_df = df[df['MRD'].str.contains('HYD')]
jai_df = df[df['MRD'].str.contains('JAI')]

print(jai_df['operating_doctor'].value_counts().head(20))

doctors_for_onboarding = ['Umesh', 'Anand Balasubramaniam', 'LOHITHASHWA G N', 'Sowmya R', 'Vidhya C', 'Venkata Prabhalar Guduru', 'Macwana Palak Niranjan', 'M P Deepika', 'Polkampally Sirisha', 'Balam Pradeep', 'Neeraj Shah', 'Jaswant Singh', 'Amit Mohan', 'Visweswaran S', 'Surabhi Khandelwal']

# print(blr_df['operating_doctor'].value_counts())

for i, row in df.iterrows():
    surgery_date = row['surgery_date']
    surgery_date = pd.to_datetime(surgery_date)
    print(f"Timestamp: {row['ts']}")
    if surgery_date < pd.to_datetime(datetime.now().date() - pd.DateOffset(days=7)):
        print("Skipping")
        continue
    if row['operating_doctor'] not in doctors_for_onboarding:
        continue
    print("Onboarding", row['MRD'])
    try:
        medics_onboard.onboard_medics_helper(row)
        # patient_table.delete_entity(row['PartitionKey'], row['RowKey'])
    except Exception as e:
        print(traceback.format_exc())
        continue