import os
import json
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

#Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChaosKafkaProducer:
    #Class for producing Kafka messages for chaos events
    def __init__(self, brokers=None, topic=None):
        self.brokers = brokers if brokers else ['localhost:30092']
        self.topic = topic if topic else 'chaos-events'
        self.producer = None
        self.connected = False
        self._connect()
    
    def _connect(self):
        #Try to connect to Kafka producer
        try:
            self.producer = KafkaProducer (
               bootstrap_servers = self.brokers,
               value_serializer = lambda v: json.dumps(v).encode('utf-8'),
               client_id = 'chaos-experiments-producer'
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

    def send_event(self, event_data):
        #Sends a JSON dictionary with event info to configured Kafka topic
        if not self.connected or not self.producer:
            logger.warning(f"Kafka producer not connected. The following event data will not be sent: {event_data.get('event_type', 'N/A')} - {event_data.get('experiment_id', 'N/A')}")
            return False
        
        try:
            logger.info(f"Sent event to Kafka: {event_data.get('event_type', 'N/A')} - {event_data.get('experiment_id', 'N/A')}")
            self.producer.flush()
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