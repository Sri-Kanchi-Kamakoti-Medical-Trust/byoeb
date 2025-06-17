import os
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

if __name__ == "__main__":
    
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=os.environ['AZ_KEY_VAULT_URL'].strip(), credential=credential)
    secret = client.get_secret("google-sheets-api")

    file = json.loads(secret.value)

    #save file to cron_job/credentials.json
    with open('cron_jobs/credentials.json', 'w') as f:
        json.dump(file, f)