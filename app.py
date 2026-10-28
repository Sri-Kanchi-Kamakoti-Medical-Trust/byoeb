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
from flask import Flask, request
from time import sleep
import traceback

sys.path.append("src")
from app_logging import LoggingDatabase
from responder import WhatsappResponder
from medics_integration import OnboardMedics
from az_table import PatientTable
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

logger = LoggingDatabase(config)
medics_onboard = OnboardMedics(config, logger)

if config["CHAT_APPLICATION"] == "whatsapp":
    responder = WhatsappResponder(config)


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

@app.route('/medics', methods=['POST'])
def medics():
    data = request.json
    for row in data:
        medics_onboard.onboard_medics_helper(row)
    return 'OK', 200

@app.route("/webhooks", methods=["POST"])
def webhook():
    body = request.json
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


def run_cron_command(command):
    """Runs one due cron.txt command and logs the outcome.

    Previously this was a bare subprocess.run() with the result discarded, so
    a crash inside e.g. generate_and_send_logs.py was invisible - the request
    still returned 200 OK. Running it in its own thread also means a slow job
    (a large onboarding batch) can't delay or starve a later one.
    """
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=1800
        )
        if result.returncode != 0:
            logger.add_log(
                sender_id="Scheduler",
                receiver_id="Bot",
                message_id=None,
                action_type="Scheduler job failed",
                details={
                    "command": command,
                    "returncode": result.returncode,
                    "stderr": result.stderr[-2000:],
                },
                timestamp=str(datetime.now()),
            )
        if "kb_update" in command:
            responder.update_kb()
    except subprocess.TimeoutExpired:
        logger.add_log(
            sender_id="Scheduler",
            receiver_id="Bot",
            message_id=None,
            action_type="Scheduler job timed out",
            details={"command": command},
            timestamp=str(datetime.now()),
        )
    except Exception as e:
        logger.add_log(
            sender_id="Scheduler",
            receiver_id="Bot",
            message_id=None,
            action_type="Scheduler job error",
            details={"command": command, "error": str(e)},
            timestamp=str(datetime.now()),
        )
        traceback.print_exc()


@app.route("/scheduler", methods=["POST"])
def scheduler():
    logger.add_log(
        sender_id="Scheduler",
        receiver_id="Bot",
        message_id=None,
        action_type="Scheduler",
        details={},
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
            # Parse the cron schedule
            parts = line.strip().split()
            if not parts:
                continue
            cron_expression = " ".join(parts[:5])
            command = " ".join(parts[5:])

            iter = croniter(cron_expression, now)
            prev_time = iter.get_prev(datetime)

            command = command.replace("$LOCAL_PATH", os.environ["APP_PATH"])

            # Due if its most recent scheduled time happened since we last checked.
            if last_check < prev_time <= now:
                due_commands.append(command)

        # Persist the check before releasing the lock, so an overlapping
        # /scheduler call can't also see the same job as due and run it twice.
        write_last_scheduler_check(now)

    # Due jobs run in background threads so a slow one can't block or skip a
    # later one in the same pass.

    for command in due_commands:
        Thread(target=run_cron_command, args=(command,), daemon=True).start()

    return "OK", 200


# Define a route for handling a POST request related to long-term processing
@app.route("/medics-sankara", methods=["POST"])
def long_term():
    data = request.json
    data = data["data"]
    logger.add_log(
        sender_id="Medics",
        receiver_id="Bot",
        message_id=None,
        action_type="Medics",
        details=data,
        timestamp=datetime.now()
    )
    outcome = {"inserted": 0, "already_exists": 0, "quarantined": 0, "failed": 0}
    error_types = []
    for row in data:
        #make all values string
        for key in row:
            row[key] = str(row[key])
        try:
            status = patient_table.insert_data(row)
        except Exception as e:
            print(e)
            traceback.print_exc()
            status = "failed"
            error_types.append(type(e).__name__)
        outcome[status] = outcome.get(status, 0) + 1
    # Counts only (no patient fields), so failed or quarantined records are visible and alertable.
    logger.add_log(
        sender_id="Medics",
        receiver_id="Bot",
        message_id=None,
        action_type="Medics intake summary",
        details={"received": len(data), **outcome, "error_types": sorted(set(error_types))},
        timestamp=datetime.now(),
    )
    return "OK", 200

@app.route("/cache-clear", methods=["POST"])
def clear_cache():
    responder.clear_cache()

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
        },
        timestamp=datetime.now(),
    )
    queue_client.delete_message(message)


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
    if config["CHAT_APPLICATION"] == "whatsapp":
        app.run()
