import os
import json
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from dotenv import load_dotenv

load_dotenv(verbose=True)

#Configure logging
logger = logging.getLogger(__name__)

class CapstoneKafkaProducer:
    #Class for producing Kafka messages
    def __init__(self, brokers=None, topic=None):
        self.brokers = "localhost:30092"
        self.topic = topic if topic else os.environ['DEFAULT_KAFKA_TOPIC']
        self.producer = None
        self.connected = False
        self._connect()

    @staticmethod
    def safe_serialize_key(key):
        try:
            return str(key).encode('utf-8') if key else None
        except Exception as e:
            logger.warning(f"Failed to serialize key: {e}")
            return None

    @staticmethod
    def safe_serialize_value(value):
        try:
            return json.dumps(value).encode('utf-8') if value else None
        except Exception as e:
            logger.warning(f"Failed to serialize value: {e}")
            return {}
        
    def _connect(self):
        #Try to connect to Kafka producer
        try:
            self.producer = KafkaProducer (
               bootstrap_servers = self.brokers,
               key_serializer=self.safe_serialize_key,
               value_serializer = self.safe_serialize_value,
               client_id = 'kafka-python-producer'
            )
            self.connected = True
            logger.info(f"Kafka producer successfully connected to {self.brokers}")
        except NoBrokersAvailable:
            logger.error(f"Unable to find brokers at {self.brokers}. Kafka messages will not be sent.")
            self.producer = None
            self.connected = False
        except Exception as e:
            logger.error(f"An unexpected error occurred during Kafka connection: {e}")
            self.producer = None
            self.connected = False

    def send_event(self, event_data, key):
        #Sends a JSON dictionary with event info to configured Kafka topic

        #Check connection
        if not self.connected:
            self._connect()

        #If still not connected - log failure
        if not self.connected or not self.producer:
            logger.warning(f"Kafka producer not connected. The following event data will not be sent: {event_data.get('event_type', 'N/A')} - {key}")
            return False
        
        try:
            self.producer.send(self.topic, 
                               key=key,
                               value=event_data)
            #self.producer.flush()
            logger.info(f"Sent event to Kafka: {event_data.get('event_type', 'N/A')} - {key}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to Kafka topc '{self.topic}': {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending event to Kafka: {e}")
            return False
        
    def close(self):
        #Closes Kafka producer
        if self.producer:
            logger.info(f"Closing Kafka producer")
            self.producer.flush()
            self.producer.close()
            self.connected = False