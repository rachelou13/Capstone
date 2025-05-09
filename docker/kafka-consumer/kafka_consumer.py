import logging
import json
import time
import socket
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKERS = [
    "kafka-0.kafka-headless.default.svc.cluster.local:9094",
    "kafka-1.kafka-headless.default.svc.cluster.local:9094",
    "kafka-2.kafka-headless.default.svc.cluster.local:9094"
]

TOPICS = ["proxy-logs", "infra-metrics", "chaos-events"]

def wait_for_kafka(brokers, timeout=5, max_attempts=20):
    logger.info("Waiting for Kafka brokers to become available...")
    for attempt in range(max_attempts):
        for broker in brokers:
            host, port = broker.split(":")
            try:
                with socket.create_connection((host, int(port)), timeout=timeout):
                    logger.info(f"Connected to Kafka broker at {broker}")
                    return
            except Exception as e:
                logger.debug(f"Broker {broker} not ready: {e}")
        logger.warning(f"Attempt {attempt + 1}/{max_attempts} - Kafka not ready. Retrying in 5 seconds...")
        time.sleep(5)
    raise NoBrokersAvailable("Kafka brokers still not available after waiting.")

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

def start_consumer():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='consumer-group-0',
        enable_auto_commit=True,
        group_id='consumer-group-0',
        key_deserializer=safe_deserialize_key,
        value_deserializer=safe_deserialize_value
    )

    logger.info("Kafka consumer started and subscribed.")

    for message in consumer:
        try:
            topic = message.topic
            value = message.value

            logger.info(f"Consumed from {topic} | Partition {message.partition} | Offset {message.offset}")
            logger.debug(f"Message content: {value}")

            if topic == 'proxy-logs':
                print(f"Sending message from {topic} to MYSQL")
            elif topic == 'infra-metrics':
                print(f"Sending message from {topic} to MONGODB")
            elif topic == 'chaos-events':
                print(f"Sending message from {topic} to SOMEWHERE")
            else:
                print(f"[UNKNOWN TOPIC] {topic}: {value}")

        except Exception as e:
            logger.error(f"Failed to process message: {e}", exc_info=True)

def main():
    try:
        wait_for_kafka(KAFKA_BROKERS)
    except NoBrokersAvailable as e:
        logger.critical(f"{e} Shutting down.")
        return

    while True:
        try:
            start_consumer()
        except KafkaError as e:
            logger.error(f"Kafka error occurred: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.critical(f"Unexpected error: {e}", exc_info=True)
            time.sleep(5)

if __name__ == '__main__':
    main()
