"""Patient data requests under the DPDP Act (Quadra P1-2, and erasure for P1-1).

Finds, exports or erases ONE patient's data across every place the bot keeps it,
and records each request (with counts, not content) in the Cosmos collection
`data_requests` as evidence.

Run only on the server (Kudu/SSH) or with the production settings, after the
request has been approved under Sankara's process:

    python3 processing/patient_data_request.py find   --phone 9876543210
    python3 processing/patient_data_request.py export --phone 9876543210 --ref GR-2026-001 --out /tmp/p.json
    python3 processing/patient_data_request.py erase  --phone 9876543210 --ref GR-2026-001 --confirm

--mrd SEHBLR/123/26 can be used instead of --phone.

Places covered:
  Cosmos: users, user_conversations, bot_conversations (patient's messages and doctors'
  polls about them), expert_conversations (doctor replies about them), logs
  Storage: PatientData table, voice notes, failed-message (dead-letter) queue
Not covered, and why:
  Google Sheet  - rebuilt from Cosmos every day at 17:00, so erased data disappears then.
  App Insights  - no phone numbers or message text since 16 Nov 2026; older entries
                  expire automatically after 90 days.
  catbot-audit  - holds no personal data (pseudonymised ids and fingerprints only).
"""
import os
import re
import sys
import json
import hmac
import hashlib
import argparse
import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PATIENT_COLLECTIONS = ("users", "user_conversations", "bot_conversations", "expert_conversations", "logs")


def normalise_phone(phone):
    """Returns (10-digit number, WhatsApp id with 91 prefix)."""
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    if len(digits) != 10:
        raise ValueError("Give a 10-digit Indian mobile number (with or without 91)")
    return digits, "91" + digits


def pseudonym(value):
    key = (os.environ.get("LOG_HASH_KEY") or "").strip()
    if not value:
        return None
    if not key:
        return "sha256:" + hashlib.sha256(value.encode()).hexdigest()[:16]
    return "phone:" + hmac.new(key.encode(), re.sub(r"\D", "", value).encode(), hashlib.sha256).hexdigest()[:12]


class Stores:
    """Every place a patient's data lives. Built from production settings, or passed fakes in tests."""

    def __init__(self, db, collections, patient_table, audio_container, dead_letter_queue):
        self.db = db
        self.col = collections
        self.patient_table = patient_table
        self.audio = audio_container
        self.dead_letter = dead_letter_queue

    @classmethod
    def from_environment(cls):
        import yaml
        import pymongo
        from azure.data.tables import TableClient
        from azure.storage.blob import BlobServiceClient
        from azure.storage.queue import QueueClient
        config = yaml.safe_load(open(os.path.join(ROOT, "config.yaml")))
        db = pymongo.MongoClient(os.environ["COSMOS_DB_CONNECTION_STRING"])[config["COSMOS_DB_NAME"]]
        names = {
            "users": config["COSMOS_USER_COLLECTION"],
            "user_conversations": config["COSMOS_USER_CONV_COLLECTION"],
            "bot_conversations": config["COSMOS_BOT_CONV_COLLECTION"],
            "expert_conversations": config["COSMOS_EXPERT_CONV_COLLECTION"],
            "logs": config["COSMOS_LOGGING_COLLECTION"],
        }
        cs = os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip()
        return cls(
            db,
            {k: db[v] for k, v in names.items()} | {"data_requests": db["data_requests"]},
            TableClient.from_connection_string(cs, "PatientData"),
            BlobServiceClient.from_connection_string(cs).get_container_client(config["AZURE_BLOB_CONTAINER_NAME"].strip()),
            QueueClient.from_connection_string(cs, os.environ["AZURE_QUEUE_NAME"].strip() + "-deadletter"),
        )


def locate(stores, phone=None, mrd=None):
    """Everything held about one patient: {store: list of records}."""
    users = stores.col["users"]
    if phone:
        digits, wa_id = normalise_phone(phone)
        user = users.find_one({"whatsapp_id": wa_id, "user_type": "Patient"})
    else:
        user = users.find_one({"patient_id": mrd, "user_type": "Patient"})
        wa_id = user["whatsapp_id"] if user else None
        digits = wa_id[2:] if wa_id else None
    found = {name: [] for name in PATIENT_COLLECTIONS + ("PatientData", "voice_notes", "dead_letter_queue")}
    found["_ids"] = {"user_id": user.get("user_id") if user else None, "whatsapp_id": wa_id, "phone": digits}
    if user:
        uid = user["user_id"]
        found["users"] = [user]
        convs = list(stores.col["user_conversations"].find({"user_id": uid}))
        found["user_conversations"] = convs
        msg_ids = [c["message_id"] for c in convs if c.get("message_id")]
        found["bot_conversations"] = list(stores.col["bot_conversations"].find(
            {"$or": [{"receiver_id": uid}, {"transaction_message_id": {"$in": msg_ids}}]}))
        found["expert_conversations"] = list(stores.col["expert_conversations"].find(
            {"transaction_message_id": {"$in": msg_ids}}))
        found["voice_notes"] = sorted({c["audio_blob_path"] for c in convs if c.get("audio_blob_path")})
    ids = [v for v in (wa_id, found["_ids"]["user_id"]) if v]
    if ids:
        found["logs"] = list(stores.col["logs"].find({"$or": [{"sender_id": {"$in": ids}}, {"receiver_id": {"$in": ids}}]}))
    if digits:
        found["PatientData"] = list(stores.patient_table.query_entities(f"RowKey eq '{digits}'"))
        found["dead_letter_queue"] = [m for m in stores.dead_letter.receive_messages(
            messages_per_page=32, visibility_timeout=60) if wa_id in m.content]
    return found


def counts(found):
    return {k: len(v) for k, v in found.items() if not k.startswith("_")}


def record_request(stores, kind, ref, found, extra=None):
    """Evidence record: what was asked, when, by whom and what was found/erased. No patient content."""
    stores.col["data_requests"].insert_one({
        "request_type": kind,
        "reference": ref,
        "user_id": found["_ids"]["user_id"],
        "phone_pseudonym": pseudonym(found["_ids"]["phone"]),
        "counts": counts(found),
        "handled_at": datetime.datetime.now(),
        "handled_by": os.environ.get("REQUEST_OPERATOR") or os.environ.get("USER") or "unknown",
        **(extra or {}),
    })


def export(found):
    """Plain JSON copy of everything held (for an access request)."""
    def clean(v):
        if isinstance(v, dict):
            return {k: clean(x) for k, x in v.items() if k not in ("_id", "_metadata")}
        if isinstance(v, list):
            return [clean(x) for x in v]
        if isinstance(v, (datetime.datetime, datetime.date)):
            return v.isoformat()
        return v if isinstance(v, (str, int, float, bool, type(None))) else str(v)
    out = {k: clean(v) for k, v in found.items() if k not in ("_ids", "dead_letter_queue")}
    out["dead_letter_queue"] = [json.loads(m.content) for m in found["dead_letter_queue"]]
    out["voice_notes"] = found["voice_notes"]
    return out


def erase(stores, found):
    """Deletes everything found. Returns per-store deletion counts."""
    done = {}
    for name in PATIENT_COLLECTIONS:
        ids = [d["_id"] for d in found[name]]
        done[name] = stores.col[name].delete_many({"_id": {"$in": ids}}).deleted_count if ids else 0
    done["PatientData"] = 0
    for e in found["PatientData"]:
        stores.patient_table.delete_entity(e["PartitionKey"], e["RowKey"])
        done["PatientData"] += 1
    done["voice_notes"] = 0
    for name in found["voice_notes"]:
        try:
            stores.audio.delete_blob(name)
            done["voice_notes"] += 1
        except Exception:
            pass
    done["dead_letter_queue"] = 0
    for m in found["dead_letter_queue"]:
        stores.dead_letter.delete_message(m)
        done["dead_letter_queue"] += 1
    return done


def main(argv=None, stores=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=["find", "export", "erase"])
    who = p.add_mutually_exclusive_group(required=True)
    who.add_argument("--phone")
    who.add_argument("--mrd")
    p.add_argument("--ref", help="Request reference (e.g. grievance number). Required for export and erase.")
    p.add_argument("--out", help="Output file for export")
    p.add_argument("--confirm", action="store_true", help="Required for erase")
    a = p.parse_args(argv)
    if a.action != "find" and not a.ref:
        p.error("--ref is required for export and erase")
    if a.action == "erase" and not a.confirm:
        p.error("erase deletes data permanently; add --confirm")
    if a.action == "export" and not a.out:
        p.error("--out is required for export")

    stores = stores or Stores.from_environment()
    found = locate(stores, phone=a.phone, mrd=a.mrd)
    if not found["_ids"]["user_id"] and not found["PatientData"]:
        print("No data found for this patient.")
        return 0
    print("Found:", json.dumps(counts(found)))
    if a.action == "find":
        return 0
    if a.action == "export":
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(export(found), f, ensure_ascii=False, indent=1)
        record_request(stores, "access", a.ref, found)
        print(f"Exported to {a.out}. Give it to the patient through the approved channel, then delete the file.")
        return 0
    deleted = erase(stores, found)
    record_request(stores, "erasure", a.ref, found, {"deleted": deleted})
    print("Erased:", json.dumps(deleted))
    print("The Google Sheet drops this patient at the next 17:00 run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
