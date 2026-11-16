import os
import re
import hmac
import hashlib
import datetime
import pymongo
import certifi
import json
from azure.monitor.opentelemetry import configure_azure_monitor
from azure.monitor.events.extension import track_event

# Quadra P0-6: Application Insights gets no message text and no phone numbers.
# Free-text fields are replaced by their length; numbers by a keyed hash, so the
# same person can still be followed across events while debugging.
CONTENT_KEYS = {
    "text", "message", "body", "query", "answer", "suggestion_text", "suggestions",
    "system_prompt", "query_prompt", "chunks", "options", "correction", "response",
}
PHONE_RE = re.compile(r"\+?\d[\d ]{8,}\d")


def pseudonym(value):
    key = (os.environ.get("LOG_HASH_KEY") or "").strip()
    if not key:
        return "phone:redacted"
    return "phone:" + hmac.new(key.encode(), re.sub(r"\D", "", value).encode(), hashlib.sha256).hexdigest()[:12]


def scrub(value, key=None):
    """Telemetry-safe copy of a log value."""
    if key in CONTENT_KEYS and value not in (None, ""):
        return f"<removed {len(str(value))} chars>"
    if isinstance(value, dict):
        return {k: scrub(v, k) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [scrub(v) for v in value]
    if isinstance(value, str):
        return PHONE_RE.sub(lambda m: pseudonym(m.group(0)) if len(re.sub(r"\D", "", m.group(0))) >= 10 else m.group(0), value)
    return value


class AppLogger:
    def __init__(self):
        
        # Events never sent to Application Insights (was misspelt and never applied).
        self.blacklist_events = ['send_reaction']

        configure_azure_monitor(
            logger_name=os.environ["APPINSIGHT_LOGGER"],
            connection_string=os.environ["APPINSIGHT_CONNECTION_STRING"]
        )

        # trace.set_tracer_provider(TracerProvider())
        # self.tracer = trace.get_tracer(__name__)

    def add_event(self,
        event_name,
        event_properties):
        if event_name in self.blacklist_events:
            return
        track_event(event_name, event_properties)
        return

    def add_log(self,
        event_name,
        **kwargs):
        if event_name in self.blacklist_events:
            return
        kwargs = {k: scrub(v, k) for k, v in kwargs.items()}
        kwargs['details'] = json.dumps(kwargs['details'])
        #remove if anything is None
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        track_event(event_name, kwargs)
        return


class LoggingDatabase:
    def __init__(self, config) -> None:
        self.client = pymongo.MongoClient(os.environ["COSMOS_DB_CONNECTION_STRING"], tlsCAFile=certifi.where())
        self.db = self.client[config["COSMOS_DB_NAME"]]
        self.collection = self.db[config["COSMOS_LOGGING_COLLECTION"]]
        self.app_logger = AppLogger()
        self.audit_trail = AuditTrail()

    def add_log(
        self,
        sender_id: str,
        receiver_id: str,
        message_id: str,
        action_type: str,
        details: str,
        timestamp: datetime.datetime,
    ) -> pymongo.results.InsertOneResult:
        """Creates a row in the collection.

        Args:
        """
        action_types = ['poll_response', 'send_message_template', 'Updating KnowledgeBase', 'send_correction', 'doctor reminder response']
        if action_type not in action_types:
            self.app_logger.add_log(action_type, details=details, sender_id=sender_id, receiver_id=receiver_id, message_id=message_id)
            return
        one_row = {
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "message_id": message_id,
            "action_type": action_type,
            "details": details,
            "timestamp": timestamp,
        }
        result = self.collection.insert_one(one_row)
        self.audit_trail.append(one_row, result.inserted_id)
        return result

    def get_log_from_message_id(self, message_id):
        return list(self.collection.find({"message_id": message_id}))


def _audit_time(ts):
    # Cosmos keeps datetimes to the millisecond (truncated), so fingerprints use the same precision.
    if isinstance(ts, datetime.datetime):
        return ts.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}"
    return str(ts)


def audit_fingerprint(row):
    """SHA-256 of an audit row's content, used to prove later that the Cosmos row was not edited."""
    content = {k: row.get(k) for k in ("sender_id", "receiver_id", "message_id", "action_type", "details")}
    content["timestamp"] = _audit_time(row.get("timestamp"))
    return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()


class AuditTrail:
    """Quadra P2-3: append-only copy of every audit event in a write-once (immutable) blob container.

    Each line holds who/what/when (phone numbers pseudonymised), the Cosmos id and the
    SHA-256 fingerprint of the full Cosmos row. No message text is stored, so the
    write-once copy never conflicts with a patient's right to erasure, yet any later
    edit or deletion of the Cosmos row can be detected (see processing/verify_audit.py).
    Never raises: auditing must not stop a reply.
    """

    def __init__(self):
        self.container = None
        try:
            from azure.storage.blob import BlobServiceClient
            name = (os.environ.get("AUDIT_CONTAINER") or "catbot-audit").strip()
            self.container = BlobServiceClient.from_connection_string(
                os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip()).get_container_client(name)
        except Exception as e:
            print(f"Audit trail disabled: {type(e).__name__}")

    def append(self, row, cosmos_id):
        if self.container is None:
            return
        try:
            line = json.dumps({
                "cosmos_id": str(cosmos_id),
                "action_type": row.get("action_type"),
                "sender": scrub(str(row.get("sender_id"))),
                "receiver": scrub(str(row.get("receiver_id"))),
                "message_id": row.get("message_id"),
                "timestamp": str(row.get("timestamp")),
                "sha256": audit_fingerprint(row),
            }) + "\n"
            blob = self.container.get_blob_client(f"audit/{datetime.date.today().isoformat()}.jsonl")
            try:
                blob.append_block(line.encode())
            except Exception:
                blob.create_append_blob()
                blob.append_block(line.encode())
        except Exception as e:
            print(f"Audit append failed: {type(e).__name__}")