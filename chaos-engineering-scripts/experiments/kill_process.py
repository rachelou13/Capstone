import psutil
import os
import sys
import time
import argparse
import logging

def find_and_kill_process(pattern):
    global killed_process
    killed_process = False
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if process.info['cmdline']:
                process_cmdline = " ".join(process.info['cmdline'])
                if pattern in process_cmdline and process.pid != os.getpid():
                    try:
                        process.terminate()
                        print(f"Sent kill signal to process {process.info['name']} {process.pid}")
                        killed_process = True
                    except psutil.NoSuchProcess:
                        print(f"Process {process.pid} disappeared before script could kill it.")
                    except psutil.AccessDenied:
                        print(f"You don't have the permissions to kill process {process.pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
        except Exception as e:
            print(f"Could not inspect process {process.pid if process else 'N/A'}: {e}")
        

def main():
    parser = argparse.ArgumentParser(description="Find and kill processes based on command-line pattern")

    parser.add_argument(
        "-p",
        "--cmdline-pattern",
        required=True,
        type=str,
        metavar="PATTERN",
        help="String pattern to search for within processes - Be very specific!"
    )

    args = parser.parse_args()

    pattern = args.cmdline_pattern

    find_and_kill_process(pattern)

    if not killed_process:
        print(f"No running processes found matching '{pattern}' (or failed to kill processes)")
    else:
        print(f"Finished finding and killing processes matching '{pattern}'")

if __name__ == "__main__":
    main()