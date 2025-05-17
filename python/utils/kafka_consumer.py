import logging
import json
import socket
import time
import mysql.connector
from pymongo import MongoClient
from kafka.consumer import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_mongodb_with_retry(host, user, password, max_attempts=20):
    logger.info(f"Attempting to connect to MongoDB at {host}...")
    for attempt in range(max_attempts):
        try:
            client = MongoClient(f"mongodb://{user}:{password}@{host}:27017/", 
                                serverSelectionTimeoutMS=5000)
            # Force a connection to verify it works
            client.admin.command('ismaster')
            logger.info(f"Successfully connected to MongoDB at {host}")
            return client
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_attempts} - MongoDB connection failed: {e}")
            if attempt < max_attempts - 1:
                time.sleep(5)
    
    raise Exception(f"Failed to connect to MongoDB at {host} after {max_attempts} attempts")

def connect_with_retry(host, user, password, database, max_attempts=20):
    for attempt in range(max_attempts):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=3306,
                connection_timeout=5
            )
            logger.info(f"Successfully connected to MySQL at {host}")
            return conn
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_attempts} - MySQL connection failed: {e}")
            if attempt < max_attempts - 1:
                time.sleep(5)
    
    raise Exception(f"Failed to connect to MySQL at {host} after {max_attempts} attempts")

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
    mysql_conn = connect_with_retry("mysql-summary-records", "root", "root", "summary_db")
    mysql_cursor = mysql_conn.cursor()

    # MongoDB connection
    try:
        mongo_client = connect_mongodb_with_retry("mongodb-service", "root", "root")
        mongo_db = mongo_client["metrics_db"]
        chaos_collection = mongo_db["chaos_events"]
        proxy_logs_collection = mongo_db["proxy_logs"]
    except Exception as e:
        logger.error(f"Fatal error: Failed to connect to MongoDB: {e}")
        mongo_client = None
        mongo_db = None
        chaos_collection = None
        proxy_logs_collection = None

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

    logger.info("Kafka consumer is running")
    consumer.subscribe(TOPICS)
    logger.info("Listening to " + ", ".join(TOPICS))

    for message in consumer:
        topic = message.topic
        value = message.value

        logger.info(f"Consumed from {topic} | Partition {message.partition} | Offset {message.offset}")
        logger.debug(f"Message content: {value}")

        if topic == 'proxy-logs':
            logger.info(f"Sending message from {topic} to MongoDB")
            if proxy_logs_collection is not None:
                try:
                    log_entry = {
                        "timestamp": value.get("timestamp"),
                        "level": value.get("level"),
                        "event": value.get("event"),
                        "message": value.get("message"),
                        "direction": value.get("direction"),
                        "client_ip": value.get("client_ip"),
                        "db_target": value.get("db_target"),
                        "source": value.get("source"),
                    }
                    # This gets rid of the values that could be null so we dont insert them into mongodb
                    log_entry = {k: v for k, v in log_entry.items() if v is not None}

                    proxy_logs_collection.insert_one(log_entry)
                except Exception as e:
                    logger.error(f"Failed to insert into MongoDB: {e}")
                    try:
                        mongo_client = connect_mongodb_with_retry("mongodb-service", "root", "root")
                        mongo_db = mongo_client["metrics_db"]
                        proxy_logs_collection = mongo_db["proxy_logs"]
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect to MongoDB: {reconnect_error}")
            else:
                logger.error("MongoDB connection not available, skipping proxy log")

        elif topic == 'infra-metrics':
            logger.info(f"Sending message from {topic} to MYSQL")
            try:
                ts = value.get("timestamp")
                source = value.get("source", "infra_metrics_scraper")
                metrics = value.get("metrics", {})
                node_metrics = metrics.get("node", {})
                container_metrics = metrics.get("containers", {})
                params = value.get("parameters", {})
                node_name = params.get("node")
                pod_name = params.get("pod_name")
                pod_namespace = params.get("pod_namespace")

                query = """
                INSERT INTO infra_metrics (
                    timestamp, source,
                    cpu_percent, cpu_used, mem_percent, mem_used,
                    node_name, pod_name, pod_namespace, container_name, metric_level
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                mysql_cursor.execute(query, (
                    ts, source,
                    node_metrics.get("cpu_usage", {}).get("percent", 0.0),
                    node_metrics.get("cpu_usage", {}).get("used", 0.0),
                    node_metrics.get("memory_usage", {}).get("percent", 0.0),
                    node_metrics.get("memory_usage", {}).get("used", 0.0),
                    node_name, pod_name, pod_namespace, None, 'node'
                ))

                if container_metrics:
                    for cname, cdata in container_metrics.items():
                        mysql_cursor.execute(query, (
                            ts, source,
                            cdata.get("cpu_usage", {}).get("percent", 0.0),
                            cdata.get("cpu_usage", {}).get("used", 0.0),
                            cdata.get("memory_usage", {}).get("percent", 0.0),
                            cdata.get("memory_usage", {}).get("used", 0.0),
                            node_name, pod_name, pod_namespace, cname, 'container'
                        ))
                else:
                    mysql_cursor.execute(query, (
                        ts, source,
                        None, None, None, None,
                        node_name, pod_name, pod_namespace, None, 'container'
                    ))

                mysql_conn.commit()
                logger.info(f"Inserted node and {len(container_metrics)} container metrics for pod {pod_name}")
            except Exception as e:
                logger.error(f"Failed to insert into MySQL: {e}")

        elif topic == 'chaos-events':
            logger.info(f"sending message from {topic} to MongoDB")
            if chaos_collection is not None:
                try:
                    chaos_collection.insert_one(value)
                    logger.info("Inserted chaos event: %s from %s", value.get("event_type"), value.get("source"))
                except Exception as e:
                    logger.error(f"Failed to insert into MongoDB: {e}")
                    # Try to reconnect
                    try:
                        mongo_client = connect_mongodb_with_retry("mongodb-service", "root", "root")
                        mongo_db = mongo_client["metrics_db"]
                        chaos_collection = mongo_db["chaos_events"]
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect to MongoDB: {reconnect_error}")
            else:
                logger.error("MongoDB connection not available, skipping chaos event")
        else:
            print(f"[UNKNOWN TOPIC] {topic}: {value}")

if __name__ == '__main__':
    main()
