import os
import sys
from threading import Thread, Lock
import yaml
import logging
from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceExistsError
from datetime import datetime, timedelta
import pytz
from croniter import croniter
import subprocess
import json
import hmac
import hashlib
from flask import Flask, request
from time import sleep
import traceback

sys.path.append("src")
from app_logging import LoggingDatabase
from responder import WhatsappResponder
from medics_integration import OnboardMedics
from az_table import PatientTable, without_fields, NOT_LOGGED_FIELDS
from utils import is_older_than_n_minutes

with open("config.yaml") as file:
    config = yaml.safe_load(file)

print("Starting application")
app = Flask(__name__)
# Largest observed MEDICS request is well under 1 KB; WhatsApp webhooks are a few KB.
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024
if not (os.environ.get("VERIFY_TOKEN") or "").strip():
    print("WARNING: VERIFY_TOKEN is not set; webhook verification requests will be refused")
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)


def setting(name):
    return (os.environ.get(name) or "").strip()

logger = LoggingDatabase(config)
medics_onboard = OnboardMedics(config, logger)

if config["CHAT_APPLICATION"] == "whatsapp":
    responder = WhatsappResponder(config)


def startup_checks():
    """Records at startup whether secrets read at run time are reachable (names and results only)."""
    results = {}
    try:
        from utils import google_service_account_info
        google_service_account_info()
        results["google_sheets_key_from_key_vault"] = "ok"
    except Exception as e:
        results["google_sheets_key_from_key_vault"] = type(e).__name__
    for name in ("SCHEDULER_KEY", "META_APP_SECRET", "MEDICS_SHARED_KEY", "MEDICS_ALLOWED_IPS", "LOG_HASH_KEY"):
        results[name] = "set" if setting(name) else "not set"
    logger.add_log(sender_id="Bot", receiver_id="Bot", message_id=None, action_type="Startup check",
                   details=results, timestamp=datetime.now())


Thread(target=startup_checks, daemon=True).start()

pause_queue = False
queue_lock = Lock()
scheduler_lock = Lock()

queue_name = os.environ["AZURE_QUEUE_NAME"].strip()
queue_client = QueueClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip(), queue_name)

try:
    queue_client.create_queue()
except ResourceExistsError:
    pass

# Messages that fail processing are kept here instead of being discarded.
dead_letter_client = QueueClient.from_connection_string(
    os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip(), queue_name + "-deadletter"
)
try:
    dead_letter_client.create_queue()
except ResourceExistsError:
    pass
except Exception:
    traceback.print_exc()

DEAD_LETTER_TTL_SECONDS = 30 * 24 * 60 * 60
# Must exceed worst-case processing time so a message is not redelivered mid-processing
# (an AI call can take up to ~9 minutes in the worst case: 3 attempts x client retries x 60s).
QUEUE_VISIBILITY_TIMEOUT = 900
QUEUE_STALE_AFTER_SECONDS = 1200
last_queue_poll = datetime.now()

patient_table = PatientTable()


@app.route("/")
def index():
    print("Request for index page received")
    return "Flask is running!"

@app.route("/health")
def health():
    seconds_since_poll = int((datetime.now() - last_queue_poll).total_seconds())
    if seconds_since_poll > QUEUE_STALE_AFTER_SECONDS:
        return {"status": "queue_stalled", "seconds_since_last_poll": seconds_since_poll}, 503
    return {"status": "ok", "seconds_since_last_poll": seconds_since_poll}, 200

@app.route("/webhooks", methods=["POST"])
def webhook():
    # Quadra P0-2: Meta signs every webhook with the app secret. Checked on the raw
    # body before anything is parsed or queued. Active once META_APP_SECRET is set.
    app_secret = setting("META_APP_SECRET")
    if app_secret:
        expected = "sha256=" + hmac.new(app_secret.encode(), request.get_data(), hashlib.sha256).hexdigest()
        if not secret_matches(expected, request.headers.get("X-Hub-Signature-256")):
            reason = "missing" if not request.headers.get("X-Hub-Signature-256") else "mismatch"
            if reject("Webhook signature invalid", "META_SIGNATURE_MODE", reason=reason, ip=client_ip()):
                return "Forbidden", 403
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return "Bad Request", 400
    if (
        body.get("object")
        and body.get("entry")
        and body["entry"][0].get("changes")
        and body["entry"][0]["changes"][0].get("value")
        and body["entry"][0]["changes"][0]["value"].get("messages")
        and body["entry"][0]["changes"][0]["value"]["messages"][0]
    ):
        timestamp = body["entry"][0]["changes"][0]["value"]["messages"][0]["timestamp"]
        n = 2
        if is_older_than_n_minutes(int(timestamp), n=n):
            logger.add_log(
                sender_id="whatsapp_api",
                receiver_id="Bot",
                message_id=None,
                action_type="Old message",
                details={"message": f"Message older than {n} minutes"},
                timestamp=datetime.now(),
            )
            return "OK", 200
    # adding request to queue
    queue_client.send_message(json.dumps(body))
    return "OK", 200


SCHEDULER_STATE_FILE = "scheduler_state.json"


def read_last_scheduler_check(now):
    """Last time /scheduler finished a pass over cron.txt.

    The old logic only ran a job if this endpoint happened to be hit inside a
    5-minute window right after its scheduled time, so one missed call (slow
    request, cold start, overlapping timer) silently skipped that job for the
    whole day with no retry. Tracking the last check lets a job run as soon
    as we notice it's due, however late that is.
    """
    try:
        with open(SCHEDULER_STATE_FILE, "r") as f:
            state = json.load(f)
        return datetime.fromisoformat(state["last_check"])
    except (FileNotFoundError, KeyError, ValueError):
        # No state yet: only look back a short, safe window.
        return now - timedelta(minutes=65)


def write_last_scheduler_check(now):
    with open(SCHEDULER_STATE_FILE, "w") as f:
        json.dump({"last_check": now.isoformat()}, f)


# Quadra P0-3: cron.txt names a job; only these fixed scripts can ever run, and
# never through a shell. (cron.txt used to hold shell command strings.)
SCHEDULED_JOBS = {
    name: f"cron_jobs/{name}.py"
    for name in (
        "escalate", "onboard_users", "generate_and_send_logs", "user_reminder",
        "account_expiration", "escalate_multiple", "expert_reminder", "find_consensus",
        "kb_update", "retry_messages", "send_usage_stats",
    )
}


def log_job_problem(action_type, job, **details):
    # "command" keeps the cron_jobs/<name>.py form the job-failure alert extracts.
    logger.add_log(
        sender_id="Scheduler",
        receiver_id="Bot",
        message_id=None,
        action_type=action_type,
        details={"job": job, "command": SCHEDULED_JOBS.get(job, f"cron_jobs/{job}.py"), **details},
        timestamp=str(datetime.now()),
    )


def run_cron_command(job):
    """Runs one due scheduled job and logs the outcome.

    Previously this was a bare subprocess.run() with the result discarded, so
    a crash inside e.g. generate_and_send_logs.py was invisible - the request
    still returned 200 OK. Running it in its own thread also means a slow job
    (a large onboarding batch) can't delay or starve a later one.
    """
    script = SCHEDULED_JOBS.get(job)
    if script is None:
        log_job_problem("Scheduler job failed", job, reason="unknown_job")
        return
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(os.environ["APP_PATH"], script)],
            shell=False, capture_output=True, text=True, timeout=1800,
        )
        if result.returncode != 0:
            log_job_problem("Scheduler job failed", job, returncode=result.returncode, stderr=result.stderr[-2000:])
        if job == "kb_update":
            responder.update_kb()
    except subprocess.TimeoutExpired:
        log_job_problem("Scheduler job timed out", job)
    except Exception as e:
        log_job_problem("Scheduler job error", job, error=str(e))
        traceback.print_exc()


def client_ip():
    """Caller's IP. App Service puts the real client first in X-Forwarded-For, as ip:port."""
    ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
    if ip.startswith("["):
        return ip[1:ip.index("]")] if "]" in ip else ip
    return ip.split(":")[0] if ip.count(":") == 1 else ip


def secret_matches(expected, provided):
    return bool(provided) and hmac.compare_digest(provided.encode(), expected.encode())


def reject(action_type, mode_setting, **details):
    """Logs a refused caller. Returns True if the request must be refused.

    If <mode_setting> is "log", the call is only logged and still accepted, so a new
    check can be watched for a few days before it is enforced.
    """
    enforce = setting(mode_setting).lower() != "log"
    logger.add_log(
        sender_id="Security",
        receiver_id="Bot",
        message_id=None,
        action_type=action_type,
        details={**details, "enforced": enforce},
        timestamp=datetime.now(),
    )
    return enforce


@app.route("/scheduler", methods=["POST"])
def scheduler():
    # Quadra P0-1: only the scheduler Function knows this key (sent as ?key= in its
    # ENDPOINT_URL, or the X-Scheduler-Key header).
    scheduler_key = setting("SCHEDULER_KEY")
    if scheduler_key:
        provided = request.args.get("key") or request.headers.get("X-Scheduler-Key")
        if not secret_matches(scheduler_key, provided):
            if reject("Scheduler call rejected", "SCHEDULER_AUTH_MODE", ip=client_ip()):
                return "Forbidden", 403
    # Main-queue length for the queue-depth alert (Quadra P1-8). The storage
    # metric counts every queue in the shared account, including dead letters.
    try:
        queue_depth = queue_client.get_queue_properties().approximate_message_count
    except Exception:
        queue_depth = None
    logger.add_log(
        sender_id="Scheduler",
        receiver_id="Bot",
        message_id=None,
        action_type="Scheduler",
        details={"queue_depth": queue_depth},
        timestamp=str(datetime.now()),
    )
    # Checked before taking any lock, so it is reported even if the queue
    # loop is stuck inside a slow call.
    seconds_since_poll = int((datetime.now() - last_queue_poll).total_seconds())
    if seconds_since_poll > QUEUE_STALE_AFTER_SECONDS:
        logger.add_log(
            sender_id="Scheduler",
            receiver_id="Bot",
            message_id=None,
            action_type="Queue stalled",
            details={"seconds_since_last_poll": seconds_since_poll},
            timestamp=str(datetime.now()),
        )

    # Uses its own lock, not queue_lock: waiting on queue_lock blocked the only
    # gunicorn worker whenever a message took long, freezing every endpoint
    # until gunicorn killed the worker. Jobs run in background threads, so
    # pausing the queue here served no purpose. Always released (finally), so
    # an error here cannot leave every later /scheduler call waiting forever.
    with scheduler_lock:
        # Get the current time in IST
        now = datetime.now(pytz.timezone("Asia/Kolkata"))
        last_check = read_last_scheduler_check(now)

        # Parse the cron schedules
        with open("cron.txt", "r") as f:
            lines = f.readlines()

        due_commands = []
        for line in lines:
            # Parse the cron schedule: five time fields, then one job name.
            parts = line.strip().split()
            if not parts or parts[0].startswith("#"):
                continue
            cron_expression = " ".join(parts[:5])
            job = " ".join(parts[5:])

            iter = croniter(cron_expression, now)
            prev_time = iter.get_prev(datetime)

            # Due if its most recent scheduled time happened since we last checked.
            if last_check < prev_time <= now:
                due_commands.append(job)

        # Persist the check before releasing the lock, so an overlapping
        # /scheduler call can't also see the same job as due and run it twice.
        write_last_scheduler_check(now)

    # Due jobs run in background threads so a slow one can't block or skip a
    # later one in the same pass.

    for command in due_commands:
        Thread(target=run_cron_command, args=(command,), daemon=True).start()

    return "OK", 200


MEDICS_REQUIRED_FIELDS = ("MRD", "phone_number")


def medics_caller_problem():
    """Quadra P0-1: MEDICS is identified by its source IPs and a shared key.

    Each check is active only once its setting exists (MEDICS_ALLOWED_IPS,
    comma-separated; MEDICS_SHARED_KEY, sent as the X-Medics-Key header or ?key=).
    """
    allowed_ips = [ip.strip() for ip in setting("MEDICS_ALLOWED_IPS").split(",") if ip.strip()]
    if allowed_ips and client_ip() not in allowed_ips:
        return "ip_not_allowed"
    key = setting("MEDICS_SHARED_KEY")
    if key and not secret_matches(key, request.headers.get("X-Medics-Key") or request.args.get("key")):
        return "bad_key"
    return None


# Define a route for handling a POST request related to long-term processing
@app.route("/medics-sankara", methods=["POST"])
def long_term():
    source_ip = client_ip()
    problem = medics_caller_problem()
    if problem and reject("Medics request rejected", "MEDICS_AUTH_MODE", reason=problem, ip=source_ip):
        return "Forbidden", 403

    # Payload shape check: a JSON object whose "data" is a list of records.
    payload = request.get_json(silent=True)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or not all(isinstance(row, dict) for row in data):
        logger.add_log(
            sender_id="Medics",
            receiver_id="Bot",
            message_id=None,
            action_type="Medics payload rejected",
            details={"ip": source_ip, "reason": "not a JSON object with a list of records in 'data'"},
            timestamp=datetime.now(),
        )
        return "Bad Request: expected {\"data\": [ {...}, ... ]}", 400

    logger.add_log(
        sender_id="Medics",
        receiver_id="Bot",
        message_id=None,
        action_type="Medics",
        details=[without_fields(row, NOT_LOGGED_FIELDS) for row in data],
        timestamp=datetime.now()
    )
    outcome = {"inserted": 0, "already_exists": 0, "quarantined": 0, "failed": 0}
    error_types = []
    missing_fields = 0
    for row in data:
        #make all values string
        for key in row:
            row[key] = str(row[key])
        if any(not row.get(f) for f in MEDICS_REQUIRED_FIELDS):
            missing_fields += 1
        try:
            status = patient_table.insert_data(row)
        except Exception as e:
            print(e)
            traceback.print_exc()
            status = "failed"
            error_types.append(type(e).__name__)
        outcome[status] = outcome.get(status, 0) + 1
    # Counts only (no patient fields), so failed or quarantined records are visible and alertable.
    # The source IP is MEDICS' server address, recorded so the IP allow-list can be confirmed.
    logger.add_log(
        sender_id="Medics",
        receiver_id="Bot",
        message_id=None,
        action_type="Medics intake summary",
        details={"received": len(data), **outcome, "error_types": sorted(set(error_types)),
                 "missing_required_fields": missing_fields, "source_ip": source_ip},
        timestamp=datetime.now(),
    )
    return "OK", 200

# /medics (direct onboarding with no checks) and /cache-clear (always returned 500)
# were removed for Quadra P0-1; MEDICS uses /medics-sankara.

# Define a route for handling webhooks
@app.route("/webhooks", methods=["GET"])
def verify_webhook():
    verify_token = (os.environ.get("VERIFY_TOKEN") or "").strip()
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if not verify_token:
        print("Webhook verification refused: VERIFY_TOKEN is not set")
        return "Forbidden", 403
    if mode and token:
        if mode == "subscribe" and hmac.compare_digest(token.encode(), verify_token.encode()):
            print("WEBHOOK_VERIFIED")
            return challenge, 200
        else:
            return "Forbidden", 403
    return "Not Found", 404


def dead_letter(message, reason, error=None):
    """Keep a copy of a message we could not process, then remove it from the main queue.

    Previously such messages were deleted outright, so a patient's message
    could be lost with no record. The dead-letter copy keeps it for review or
    re-drive. If the copy fails we still delete, as before, so a poison
    message can never block the queue.
    """
    try:
        dead_letter_client.send_message(message.content, time_to_live=DEAD_LETTER_TTL_SECONDS)
    except Exception:
        traceback.print_exc()
    logger.add_log(
        sender_id="queue_processor",
        receiver_id="Bot",
        message_id=None,
        action_type="Message dead-lettered",
        details={
            "reason": reason,
            "queue_message_id": message.id,
            "dequeue_count": message.dequeue_count,
            "error": type(error).__name__ if error else None,
            "where": error_location(error),
            # AttributeError/TypeError text names only Python types and attributes.
            "error_text": str(error)[:150] if isinstance(error, (AttributeError, TypeError)) else None,
        },
        timestamp=datetime.now(),
    )
    queue_client.delete_message(message)
    # Content-free copy in Cosmos for the daily email's received-vs-processed line (P1-6).
    try:
        logger.collection.insert_one({
            "sender_id": "queue_processor", "receiver_id": "Bot", "message_id": None,
            "action_type": "Message dead-lettered",
            "details": {"reason": reason, "queue_message_id": message.id,
                        "error": type(error).__name__ if error else None},
            "timestamp": datetime.now(),
        })
    except Exception:
        pass


def error_location(error):
    """Where an error happened as "file:line function" (last 3 frames).

    Code locations only, never message text, so it is safe to log and tells us
    which line to fix without reproducing the patient's message.
    """
    if error is None or error.__traceback__ is None:
        return None
    frames = traceback.extract_tb(error.__traceback__)[-3:]
    return [f"{os.path.basename(f.filename)}:{f.lineno} {f.name}" for f in frames]


def process_queue():
    print("Starting queue processing")
    global last_queue_poll
    # while the queue is non empty, retrieve the top massage and process it
    while True:
        # The lock is always released, even on the paused path; the old
        # acquire()/continue on that path never released it.
        with queue_lock:
            paused = pause_queue
            if not paused:
                try:
                    messages = queue_client.receive_messages(
                        messages_per_page=1, visibility_timeout=QUEUE_VISIBILITY_TIMEOUT
                    )
                    for message in messages:
                        try:
                            if message.dequeue_count > 1:
                                dead_letter(message, "dequeue_count_exceeded")
                                continue
                            body = json.loads(message.content)
                            responder.response(body)
                            queue_client.delete_message(message)
                        except Exception as e:
                            print(e)
                            traceback.print_exc()
                            dead_letter(message, "processing_error", e)
                        last_queue_poll = datetime.now()
                    last_queue_poll = datetime.now()
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    logger.add_log(
                        sender_id="queue_processor",
                        receiver_id="Bot",
                        message_id=None,
                        action_type="Queue receive error",
                        details={"error": type(e).__name__},
                        timestamp=datetime.now(),
                    )
        sleep(0.01 if paused else 0.1)


Thread(target=process_queue).start()

if __name__ == "__main__":
    # Quadra P1-10: the Flask development server must never handle real patient data.
    # Production runs through start.sh (gunicorn) on Azure App Service.
    if os.environ.get("WEBSITE_SITE_NAME") or os.environ.get("ALLOW_DEV_SERVER") != "1":
        sys.exit("Refusing to start the Flask development server. Production uses start.sh (gunicorn). "
                 "For local testing with TEST credentials only, set ALLOW_DEV_SERVER=1.")
    if config["CHAT_APPLICATION"] == "whatsapp":
        app.run()
