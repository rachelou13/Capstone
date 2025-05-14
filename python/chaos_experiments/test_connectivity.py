import socket
import sys
import time

def test_connection(target_host, port, timeout=5):
    print(f'Testing connection to {target_host}:{port}')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    start_time = time.time()
    
    try:
        result = s.connect_ex((target_host, port))
        if result == 0:
            print('CONNECTION_RESULT: SUCCESS')
        else:
            print(f'CONNECTION_RESULT: FAILED (error code {result})')
    except Exception as e:
        print(f'CONNECTION_RESULT: FAILED (exception: {e})')
    finally:
        s.close()
        print(f'Connection test took {time.time() - start_time:.2f} seconds')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python test_connectivity.py <host> <port> [timeout]")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    timeout = float(sys.argv[3]) if len(sys.argv) > 3 else 5
    
    test_connection(host, port, timeout)