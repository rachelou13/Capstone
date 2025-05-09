import time
import mysql.connector
from mysql.connector import Error
import socket
import threading
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import json
from datetime import datetime

# The port for our proxy
PROXY_PORT = 3307

# Global variables for our host aand replica names
PRIMARY_HOST = "mysql-primary"
REPLICA_HOST = "mysql-replica"

# Info for our Kafka producer
KAFKA_BROKER = ",".join([
    "kafka-0.kafka-headless.default.svc.cluster.local:9094",
    "kafka-1.kafka-headless.default.svc.cluster.local:9094",
    "kafka-2.kafka-headless.default.svc.cluster.local:9094"
])
KAFKA_TOPIC = "proxy-logs"

DB_CONFIG = {
    "user": "root",
    "password": "admin",
    "database": "capstone_db"
}

current_host = PRIMARY_HOST

#*****************************************************************************************************************************************************
# Initialize Kafka producer
try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Kafka producer ready")
except Exception as e:
    producer = None
    print(f"Kafka producer failed to initialize: {e}")

#*****************************************************************************************************************************************************

def connect_to_database(host):
    try:
        conn = mysql.connector.connect(host=host, **DB_CONFIG)
        if conn.is_connected():
            print(f"Connected to {host}")
            return conn
    except Error as e:
        print(f"Failed to connect to {host}: {e}")
    return None

#*****************************************************************************************************************************************************

def forward(source, destination, direction, client_ip):
    try:
        while True:
            data = source.recv(4096)
            if not data:
                break
            try:
                destination.sendall(data)
            except (BrokenPipeError, ConnectionResetError) as e:
                print(f"sendall() failed: {e}")
                # For when sending fails
                if producer:
                    producer.send(KAFKA_TOPIC, {
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "level": "ERROR",
                        "event": "broken_pipe",
                        "message": str(e),
                        "direction": direction,
                        "client_ip": client_ip,
                        "db_target": current_host,
                        "source": "proxy-server"
                    })
                break
    except (ConnectionResetError, OSError) as e:
        print(f"recv() failed: {e}")
        # For when the reading fails
        if producer:
            producer.send(KAFKA_TOPIC, {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": "ERROR",
                "event": "recv_failed",
                "message": str(e),
                "direction": direction,
                "client_ip": client_ip,
                "db_target": current_host,
                "source": "proxy-server"
            })
    finally:
        try:
            source.shutdown(socket.SHUT_RDWR)
        except:
            pass
        try:
            destination.shutdown(socket.SHUT_RDWR)
        except:
            pass
        source.close()
        destination.close()

#*****************************************************************************************************************************************************

def switch_to_other():
    global current_host

    other_host = REPLICA_HOST if current_host == PRIMARY_HOST else PRIMARY_HOST

    if connect_to_database(other_host):
        if other_host == REPLICA_HOST:
            print("Primary is down. Promoting replica...")
            promote_to_primary(REPLICA_HOST)

            if producer:
                producer.send(KAFKA_TOPIC, {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "level": "WARNING",
                    "event": "failover",
                    "db_target": REPLICA_HOST,
                    "source": "proxy-server"
                })
        else:
            print("Switching back to primary...")
            configure_as_replica(REPLICA_HOST, PRIMARY_HOST)

            if producer:
                producer.send(KAFKA_TOPIC, {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "level": "INFO",
                    "event": "reconnected",
                    "db_target": PRIMARY_HOST,
                    "source": "proxy-server"
                })

        current_host = other_host
        print(f"Now using {current_host} as active DB")
    else:
        print(f"Could not connect to {other_host}. Staying with {current_host}")


#*****************************************************************************************************************************************************

def monitor_and_failover():
    global current_host

    while True:
        print(f"Checking {current_host}...")
        connection = connect_to_database(current_host)

        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")

                if producer:
                    producer.send(KAFKA_TOPIC, {
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "level": "INFO",
                        "event": "up",
                        "db_target": current_host,
                        "source": "proxy-server"
                    })

                print(f"{current_host} is alive")

                # If we’re currently on the replica, check if the primary has recovered
                if current_host == REPLICA_HOST:
                    print("Checking if primary is back...")
                    if connect_to_database(PRIMARY_HOST):
                        print("Primary is back. Reconfiguring...")
                        configure_as_replica(REPLICA_HOST, PRIMARY_HOST)
                        current_host = PRIMARY_HOST

                time.sleep(10)

            except Error as e:
                print(f"DB error on {current_host}: {e}")
                connection.close()

                if producer:
                    producer.send(KAFKA_TOPIC, {
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "level": "ERROR",
                        "event": "down",
                        "db_target": current_host,
                        "source": "proxy-server"
                    })

                switch_to_other()
        else:
            print(f"Connection failed for {current_host}. Switching...")

            if producer:
                producer.send(KAFKA_TOPIC, {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "level": "ERROR",
                    "event": "down",
                    "db_target": current_host,
                    "source": "proxy-server"
                })

            switch_to_other()

        time.sleep(5)


#*****************************************************************************************************************************************************

def handle_client(client_socket):
    global current_host
    try:
        db_socket = socket.create_connection((current_host, 3306))
        client_ip = client_socket.getpeername()[0]
        if producer:
            producer.send(KAFKA_TOPIC, {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": "INFO",
                "event": "connection_accepted",
                "client_ip": client_ip,
                "db_target": current_host,
                "source": "proxy-server"
            })
        
        # Start bidirectional forwarding
        threading.Thread(target=forward, args=(client_socket, db_socket, "client->db", client_ip)).start()
        threading.Thread(target=forward, args=(db_socket, client_socket, "db->client", client_ip)).start()

    except Exception as e:
        print(f"Connection failed: {e}")
        client_socket.close()

#*****************************************************************************************************************************************************

def start_proxy():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("", PROXY_PORT))
    server.listen(5)

    print(f"Proxy listening on port {PROXY_PORT}")

    while True:
        client_socket, client_address = server.accept()
        print(f"Accepted connection from {client_address}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

#*****************************************************************************************************************************************************

def promote_to_primary(host):
    try:
        conn = mysql.connector.connect(host=host, user="root", password="admin")
        cursor = conn.cursor()
        cursor.execute("STOP SLAVE;")
        cursor.execute("RESET SLAVE ALL;")
        cursor.execute("SET GLOBAL read_only = OFF;")
        conn.close()
        print(f"Promoted {host} to primary")
    except Error as e:
        print(f"Failed to promote {host} to primary: {e}")

#*****************************************************************************************************************************************************

def configure_as_replica(replica_host, master_host):
    try:
        conn = mysql.connector.connect(host=replica_host, user="root", password="admin")
        cursor = conn.cursor()
        cursor.execute("STOP SLAVE;")
        cursor.execute(f"""
            CHANGE MASTER TO
              MASTER_HOST='{master_host}',
              MASTER_USER='replica_user',
              MASTER_PASSWORD='replica_password',
              MASTER_AUTO_POSITION=1;
        """)
        cursor.execute("START SLAVE;")
        conn.close()
        print(f"Configured {replica_host} as replica of {master_host}")
    except Error as e:
        print(f"Failed to configure {replica_host} as replica: {e}")

#*****************************************************************************************************************************************************

if __name__ == "__main__":
    threading.Thread(target=monitor_and_failover, daemon=True).start()
    start_proxy()
