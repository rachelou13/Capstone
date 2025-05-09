import logging
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_deserialize_key(key):
    try:
        return key.decode('utf-8') if key else None
    except Exception as e:
        logger.warning(f"Failed to decode key: {e}")
        return None

def safe_deserialize_value(value):
    try:
        return json.loads(value.decode('utf-8')) if value else None
    except Exception as e:
        logger.warning(f"Failed to deserialize value: {e}")
        return {}

def main():
    KAFKA_BROKERS = [
    "kafka-0.kafka-headless.default.svc.cluster.local:9094",
    "kafka-1.kafka-headless.default.svc.cluster.local:9094",
    "kafka-2.kafka-headless.default.svc.cluster.local:9094"
    ]

    TOPICS = ["proxy-logs", "infra-metrics", "chaos-events"]

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,
        enable_auto_commit=True,
        group_id='consumer-group-live',
        key_deserializer=safe_deserialize_key,
        value_deserializer=safe_deserialize_value
    )
    
    # Need to make the tables for the info if they are not created already so we can store the log in the databases

    print("Kafka consumer is running")

    consumer.subscribe(TOPICS)

    print("Listeninig to proxy-logs, infra-metrics, and chaos-events")

    for message in consumer:
        logger.info(f"Consumed message {message.value} from topic {message.topic} from partition {message.partition} at offset {message.offset}")
        topic = message.topic
        value = message.value

        if topic == 'proxy-logs':
            # send to mysql in a nice way
            print(f"Sending message from {topic} to MYSQL")
        elif topic == 'infra-metrics':
            # send to mongodb here
            print(f"Sending message from {topic} to MONGODB")
        elif topic == 'chaos-events':
            print(f"sending message from {topic} to SOMEWHERE")
        else:
            print(f"[UNKNOWN TOPIC] {topic}: {value}")

if __name__ == '__main__':
  main()
