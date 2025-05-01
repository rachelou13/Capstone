import time
import mysql.connector
from mysql.connector import Error
import socket
import threading

# The port for our proxy
PROXY_PORT = 3307

PRIMARY_HOST = "mysql-primary"
REPLICA_HOST = "mysql-replica"

DB_CONFIG = {
    "user": "root",
    "password": "admin",
    "database": "capstone_db"
}

current_host = PRIMARY_HOST

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

def forward(source, destination):
    try:
        while True:
            data = source.recv(4096)
            if not data:
                break
            destination.sendall(data)
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        print(f"Forwarding error: {e}")
    finally:
        try:
            source.close()
            destination.close()
        except:
            pass

#*****************************************************************************************************************************************************

def switch_to_other():
    global current_host
    other_host = REPLICA_HOST if current_host == PRIMARY_HOST else PRIMARY_HOST

    if connect_to_database(other_host):
        if other_host == REPLICA_HOST:
            print("Promoting replica...")
            promote_to_primary(REPLICA_HOST)
        else:
            print("Primary is back — reconfiguring it as replica")
            configure_as_replica(PRIMARY_HOST, REPLICA_HOST)

        current_host = other_host
        print(f"Now using {current_host} as active DB")
    else:
        print(f"Could not connect to either DB. Waiting...")

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
                print(f"{current_host} is alive")
                time.sleep(10)
            except Error as e:
                print(f"DB error on {current_host}: {e}")
                connection.close()
                switch_to_other()
        else:
            print(f"Connection failed for {current_host}. Switching...")
            switch_to_other()

        time.sleep(5)

#*****************************************************************************************************************************************************

def handle_client(client_socket):
    global current_host
    try:
        db_socket = socket.create_connection((current_host, 3306))

        # Start bidirectional forwarding
        threading.Thread(target=forward, args=(client_socket, db_socket)).start()
        threading.Thread(target=forward, args=(db_socket, client_socket)).start()

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
