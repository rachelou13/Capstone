import logging
import psutil
import argparse
from chaos_engineering_scripts.utils.kafka_producer import ChaosKafkaProducer
import uuid
from datetime import datetime, timezone
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_and_terminate_process(target):
    logger.info(f"Attempting to find and terminate process matching {target}")
    global process_terminated
    process_terminated = False
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            #Filter out processes with None in cmdline
            if p.info['cmdline']:
                #Join cmdline as string for easier processing
                process_cmdline = " ".join(p.info['cmdline'])
                if target in process_cmdline and p.pid != os.getpid():
                    try:
                        #If process matches target, try to terminate process
                        p.terminate()
                        logger.info(f"Sent terminate signal to process {p.info['name']} {p.pid}")
                        process_terminated = True
                    except psutil.NoSuchProcess:
                        logger.error(f"Process {p.pid} disappeared before script could terminate it.")
                    except psutil.AccessDenied:
                        logger.error(f"You don't have the permissions to terminate process {p.pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
        except Exception as e:
            logger.error(f"Could not inspect process {p.pid if p else 'N/A'}: {e}")
        

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Find and terminate processes based on command-line pattern")
    parser.add_argument(
        "-t",
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
        "parameters": {
            "target": target
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event):
        logger.warning(f"Failed to send start event to Kafka. See log for error.")

    try:
        find_and_terminate_process(target)
    except Exception as e:
        logger.error(f"An unexpected error occurred while terminating process(es): {e}")
    #Ensure end event is always sent, kafka producer is always closed
    finally:
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "experiment_type": "terminate_process",
            "parameters": {
                "target": target
            },
            "success": process_terminated,
            "duration": (end_time - start_time).total_seconds()
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event):
                logger.warning(f"Failed to end start event to Kafka. See log for error.")

        kafka_prod.close()

        if not process_terminated:
            logger.warning(f"No running processes found matching '{target}' (or failed to terminate processes)")
        else:
            logger.info(f"Finished finding and terminating processes matching '{target}'")

if __name__ == "__main__":
    main()