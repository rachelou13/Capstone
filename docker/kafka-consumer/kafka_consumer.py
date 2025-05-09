import logging
import json
from kafka.consumer import KafkaConsumer

#Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__file__)

def main(): 
    consumer = KafkaConsumer(
        bootstrap_servers="kafka-0.kafka-headless.default.svc.cluster.local:9094,kafka-1.kafka-headless.default.svc.cluster.local:9094,kafka-2.kafka-headless.default.svc.cluster.local:9094",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-0',
        key_deserializer=lambda k: k.decode('utf-8'),
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
# Need to make the tables for the info if they are not created already so we can store the log in the databases

    print("Kafka consumer is running")

    consumer.subscribe(["proxy-logs", "infra-metrics", "chaos-events"])

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