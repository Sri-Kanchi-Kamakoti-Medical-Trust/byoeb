"""Checks the audit trail (Quadra P2-3): every audit event in the write-once
catbot-audit container must still exist, unchanged, in the Cosmos logs collection.

Read-only. Run on the server (Kudu/SSH) or anywhere with the production settings:
    python3 processing/verify_audit.py [days]      (default: last 30 days)
Prints counts only. Exit code 1 if any row is missing or altered.
"""
import os
import sys
import json
import datetime
import yaml
from bson import ObjectId
import pymongo
from azure.storage.blob import BlobServiceClient

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from app_logging import audit_fingerprint  # noqa: E402

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config = yaml.safe_load(open(os.path.join(root, "config.yaml")))
days = int(sys.argv[1]) if len(sys.argv) > 1 else 30

logs = pymongo.MongoClient(os.environ["COSMOS_DB_CONNECTION_STRING"])[config["COSMOS_DB_NAME"]][config["COSMOS_LOGGING_COLLECTION"]]
container = BlobServiceClient.from_connection_string(
    os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip()).get_container_client(
    (os.environ.get("AUDIT_CONTAINER") or "catbot-audit").strip())

since = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
checked = ok = missing = altered = 0
for blob in container.list_blobs(name_starts_with="audit/"):
    if blob.name[len("audit/"):len("audit/") + 10] < since:
        continue
    for line in container.download_blob(blob.name).readall().decode().splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        checked += 1
        row = logs.find_one({"_id": ObjectId(entry["cosmos_id"])})
        if row is None:
            missing += 1
        elif audit_fingerprint(row) != entry["sha256"]:
            altered += 1
        else:
            ok += 1

print(f"audit events checked: {checked} | unchanged: {ok} | missing from Cosmos: {missing} | altered: {altered}")
sys.exit(1 if missing or altered else 0)
