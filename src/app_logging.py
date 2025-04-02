import os
import datetime
import pymongo
import certifi
import json
from azure.monitor.opentelemetry import configure_azure_monitor
from azure.monitor.events.extension import track_event

class AppLogger:
    def __init__(self):
        
        self.blaclist_events = ['send_reaction', 'Scheduler']

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
        return self.collection.insert_one(one_row)

    def get_log_from_message_id(self, message_id):
        return list(self.collection.find({"message_id": message_id}))