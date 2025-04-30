import psutil
import os
import sys
import time
import argparse
import logging
import uuid
from datetime import datetime, timezone
from ..utils.kafka_producer import ChaosKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_and_terminate_process(target):
    logger.info(f"Attempting to find and terminate process matching {target}")
    global process_terminated
    process_terminated = False
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if process.info['cmdline']:
                process_cmdline = " ".join(process.info['cmdline'])
                if target in process_cmdline and process.pid != os.getpid():
                    try:
                        process.terminate()
                        logger.info(f"Sent terminate signal to process {process.info['name']} {process.pid}")
                        process_terminated = True
                    except psutil.NoSuchProcess:
                        logger.error(f"Process {process.pid} disappeared before script could terminate it.")
                    except psutil.AccessDenied:
                        logger.error(f"You don't have the permissions to terminate process {process.pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
        except Exception as e:
            logger.error(f"Could not inspect process {process.pid if process else 'N/A'}: {e}")
        

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Find and terminate processes based on command-line pattern")
    parser.add_argument(
        "-p",
        "--target",
        required=True,
        type=str,
        metavar="PATTERN",
        help="String pattern to search for within processes - Be very specific!"
    )
    args = parser.parse_args()
    target = args.target

    #Start kafka producer
    kafka_prod = ChaosKafkaProducer()
    experiment_id = str(uuid.uuid4())

    #Send start event to kafka
    start_time = datetime.now(timezone.utc)
    start_event = {
        "timestamp": start_time.isoformat(),
        "experiment_id": experiment_id,
        "event_type": "start",
        "experiment_type": "terminate_process",
        "target_process": target
    }
    kafka_prod.send_event(start_event)

    find_and_terminate_process(target)

    #Send end event to kafka
    end_time = datetime.now(timezone.utc)
    end_event = {
        "timestamp": end_time.isoformat(),
        "experiment_id": experiment_id,
        "event_type": "end",
        "experiment_type": "terminate_process",
        "target_process": target,
        "success": process_terminated,
        "duration": (end_time - start_time).total_seconds()
    }

    kafka_prod.send_event(end_event)
    kafka_prod.close()

    if not process_terminated:
        logger.warning(f"No running processes found matching '{target}' (or failed to terminate processes)")
    else:
        logger.info(f"Finished finding and terminating processes matching '{target}'")

if __name__ == "__main__":
    main()