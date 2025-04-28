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
    "database": "" # The name of the database goes here
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
"""
This function is our main driver code that is called

It checks if the primary host is up every 10 seconds.

If it isn't, it then switches to the replica host ensuring high avalibility
"""
def monitor_and_failover():
    # Grab the current host
    global current_host

    # Variable for the connection to our database
    connection = None

    while True:
        print(f"Trying {current_host}...")

        # Try to connect
        connection = connect_to_database(current_host)

        # If it connects, then wait 10 seconds and try again
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                print("DB is alive")

                # Wait a bit before next check
                time.sleep(10)

            except Error as e:
                print(f"Error: {e}, switching...")
                connection.close()
                current_host = REPLICA_HOST if current_host == PRIMARY_HOST else PRIMARY_HOST

        # If database is down, then switch to our replica
        else:
            print(f"Connection failed. Switching...")

            # Set the current host to the replica if our current host is the primary since it would be the one that failed. If it isn't then we set it back to primary since the replica must've failed
            current_host = REPLICA_HOST if current_host == PRIMARY_HOST else PRIMARY_HOST

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





# Initiates our driver code
if __name__ == "__main__":
    monitor_and_failover()
