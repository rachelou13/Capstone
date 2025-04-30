import time
import mysql.connector
from mysql.connector import Error
import socket
import threading

# The port for our proxy
PROXY_PORT = 3307

# Some global variables for our db names
PRIMARY_HOST = "mysql-primary" 
REPLICA_HOST = "mysql-replica"

# Our db configurations that I'll eventually get from a config file
DB_CONFIG = {
    "user": "root",
    "password": "admin",
    "database": "capstone_db"
}

# Set the primary host as the default
current_host = PRIMARY_HOST

"""
This function connects to a database based on the host we pass into it. It uses the mysql connector class for easy connection 
"""
def connect_to_database(host):
    try:
        conn = mysql.connector.connect(host=host,**DB_CONFIG)

        if conn.is_connected():
            print(f"Connected to {host}")
            return conn
    except Error as e:
        print(f"Failed to connect to {host}: {e}")
    return None

#*****************************************************************************************************************************************************

def switch_to_other():
    global current_host

    other_host = REPLICA_HOST if current_host == PRIMARY_HOST else PRIMARY_HOST

    # Try connecting to the other host
    if connect_to_database(other_host):
        # Promote new one if it's not already master
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
"""
This function is our main driver code that is called

It checks if the primary host is up every 10 seconds.

If it isn't, it then switches to the replica host ensuring high avalibility
"""
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

"""
This function handles the between the client and the database by opening a socket 

"""
def handle_client(client_socket):

    # Our global variable that tells us what host to connect to based on if the primary one is down
    global current_host

    try:
        # Connect to the current healthy database
        db_socket = socket.create_connection((current_host, 3306))

        # Now forward data in both directions
        def forward(source, destination):
            while True:
                data = source.recv(4096)
                if not data:
                    break
                destination.sendall(data)
        
        # So client can sent queries to db (takes data from client and sends it to db)
        threading.Thread(target=forward, args=(client_socket, db_socket)).start()

        # So client can recieve responses from the db (taks data from db and sends to client)
        threading.Thread(target=forward, args=(db_socket, client_socket)).start()

    # if failed, throw an error and close the socket
    except Exception as e:
        print(f"Connection failed: {e}")
        client_socket.close()

#*****************************************************************************************************************************************************
"""
This function starts the proxy server and listens for connections, if one is created, then a thread is created to handle that client
"""
def start_proxy():
    # Create a socket that accepts IPv4 addresses through TCP
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Listen for all IP addresses on our proxy port
    server.bind(("", PROXY_PORT))

    # Max queued connections
    server.listen(5)

    print(f"Proxy listening on port {PROXY_PORT}")

    while True:
        # wait for a client to connect (this blocks the loop btw)
        client_socket, client_address = server.accept()

        print(f"Accepted connection from {client_address}")

        # use the handle client function which we made to allow the client to connect to the db
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))

        # Start the thread
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


#*****************************************************************************************************************************************************
# Initiates our driver code
if __name__ == "__main__":
    # This one monitors the primary client to see if it's active and healthy
    threading.Thread(target=monitor_and_failover, daemon=True).start()

    # This allows clients to proxy through our program and communicate through the socket, the thread above automatically switches the db
    start_proxy()
