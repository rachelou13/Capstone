import logging
import json
import socket
import time
import pymysql
from pymongo import MongoClient
from kafka.consumer import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

#Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def main():
    KAFKA_BROKERS = ["kafka-0.kafka-headless.default.svc.cluster.local:9094"]

    TOPICS = ["proxy-logs", "infra-metrics", "chaos-events"]

    # MySQL connection
    mysql_conn = pymysql.connect(
        host="mysql-summary-records",
        user="root",
        password="root",
        database="summary_db"
    )
    mysql_cursor = mysql_conn.cursor()

    # MongoDB connection
    mongo_client = MongoClient("mongodb://root:root@mongodb-service:27017/")
    mongo_db = mongo_client["metrics_db"]
    chaos_collection = mongo_db["chaos_events"]

    try:
        wait_for_kafka(KAFKA_BROKERS)
    except NoBrokersAvailable as e:
        logger.error(f"Failed to connect to Kafka after retries: {e}")
        return

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-0',
        key_deserializer=safe_deserialize_key,
        value_deserializer=safe_deserialize_value,
        api_version=(3, 6)
    )
    
    # Need to make the tables for the info if they are not created already so we can store the log in the databases

    logger.info("Kafka consumer is running")

    consumer.subscribe(TOPICS)

    logger.info("Listeninig to" + ", ".join(TOPICS))

    for message in consumer:
        topic = message.topic
        value = message.value

        logger.info(f"Consumed from {topic} | Partition {message.partition} | Offset {message.offset}")
        logger.debug(f"Message content: {value}")

        if topic == 'proxy-logs':
            # send to mysql in a nice way
            print(f"Sending message from {topic} to MYSQL")
        elif topic == 'infra-metrics':
            # send to mysql here
            print(f"Sending message from {topic} to MYSQL")

            try:
                ts = value.get("timestamp")
                source = value.get("source", "infra_metrics_scraper")
                experiment_detected = value.get("experiment_detected", False)
                metrics = value.get("metrics", {})
                node_metrics = metrics.get("node", {})
                container_metrics = metrics.get("containers", {})

                params = value.get("parameters", {})
                node_name = params.get("node")
                pod_name = params.get("pod_name")
                pod_namespace = params.get("pod_namespace")

                #Process node metrics
                node_cpu_percent = node_metrics.get("cpu_usage", {}).get("percent", 0.0)
                node_cpu_used = node_metrics.get("cpu_usage", {}).get("used", 0.0)
                node_mem_percent = node_metrics.get("memory_usage", {}).get("percent", 0.0)
                node_mem_used = node_metrics.get("memory_usage", {}).get("used", 0.0)

                #Insert node-level metrics
                node_query = """
                INSERT INTO infra_metrics (
                    timestamp, source, experiment_detected,
                    cpu_percent, cpu_used, mem_percent, mem_used,
                    node_name, pod_name, pod_namespace, container_name, metric_level
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                #Use NULL for container_name and 'node' for metric_level to indicate node-level metrics
                mysql_cursor.execute(node_query, (
                    ts, source, experiment_detected,
                    node_cpu_percent, node_cpu_used, node_mem_percent, node_mem_used,
                    node_name, pod_name, pod_namespace, None, 'node'
                ))
                
                #Process container metrics if they exist
                if container_metrics:
                    container_query = """
                    INSERT INTO infra_metrics (
                        timestamp, source, experiment_detected,
                        cpu_percent, cpu_used, mem_percent, mem_used,
                        node_name, pod_name, pod_namespace, container_name, metric_level
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    for container_name, container_data in container_metrics.items():
                        #Extract container CPU and memory metrics
                        container_cpu_percent = container_data.get("cpu_usage", {}).get("percent")
                        container_cpu_used = container_data.get("cpu_usage", {}).get("used")
                        container_mem_percent = container_data.get("memory_usage", {}).get("percent")
                        container_mem_used = container_data.get("memory_usage", {}).get("used")
                        
                        mysql_cursor.execute(container_query, (
                            ts, source, experiment_detected,
                            container_cpu_percent, container_cpu_used, 
                            container_mem_percent, container_mem_used,
                            node_name, pod_name, pod_namespace, container_name, 'container'
                        ))
                
                mysql_conn.commit()
                container_count = len(container_metrics) if container_metrics else 0
                logger.info(f"Inserted node metrics and {container_count} container metrics for pod {pod_name}")
            except Exception as e:
                logger.error(f"Failed to insert into MySQL: {e}")

        elif topic == 'chaos-events':
            # send to mongodb here
            print(f"sending message from {topic} to MongoDB")
            
            try:
                chaos_collection.insert_one(value)
                logger.info("Inserted chaos event: %s from %s", value.get("event_type"), value.get("experiment_type"))
            except Exception as e:
                logger.error(f"Failed to insert into MongoDB: {e}")

        else:
            print(f"[UNKNOWN TOPIC] {topic}: {value}")

if __name__ == '__main__':
  main()