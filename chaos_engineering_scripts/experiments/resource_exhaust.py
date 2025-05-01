import logging
import multiprocessing
import time
import argparse
from chaos_engineering_scripts.utils.kafka_producer import ChaosKafkaProducer
import uuid
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cpu_worker():
    i = 0
    while True:
        i+=1
        i*i

def stress_test_cpu(num_cores, duration):
    processes = []
    logger.info(f"Running load on CPU(s): utilizing {num_cores} cores")
    for _ in range(num_cores):
        try: 
            p = multiprocessing.Process(target=cpu_worker)
            p.start()
            processes.append(p)
        except Exception as e:
            logger.error(f"Failed to start cpu worker on process {p}: {e}")
    
    time.sleep(duration)

    for p in processes:
        p.terminate()
        p.join()
        if p.is_alive():
            logger.warning(f"Process {p.pid} did not terminate cleanly")
        else:
            logger.info(f"Process {p.pid} terminated successfully")

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Stress test CPU by adding load to specified numbers of cores")
    parser.add_argument(
        "-c",
        "--cores",
        required=False,
        type=int,
        metavar="CORES",
        help="Number of cores you wish to add load to. Must be less than or equal to number of cores on your machine!"
    )
    parser.add_argument(
        "-d",
        "--duration",
        required=False,
        type=int,
        metavar="SECONDS",
        help="Duration you want the CPU stress test to run for (in seconds)"
    )
    args = parser.parse_args()
    num_cores = args.cores if args.cores and args.cores <= multiprocessing.cpu_count() else multiprocessing.cpu_count()
    duration = args.duration if args.duration else 15

    #Start kafka producer
    kafka_prod = ChaosKafkaProducer()
    experiment_id = str(uuid.uuid4())

    #Send start event to kafka
    start_time = datetime.now(timezone.utc)
    start_event = {
        "timestamp": start_time.isoformat(),
        "experiment_id": experiment_id,
        "event_type": "start",
        "experiment_type": "resource_exhaustion",
        "parameters": {
            "cpu_cores": num_cores,
            "spec_duration": duration
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event):
        logger.warning(f"Failed to send start event to Kafka. See log for error.")

    try:
        stress_test_cpu(num_cores, duration)
    except Exception as e:
        logger.error(f"An unexpected error occurred while running load on CPU(s): {e}")
    #Ensure end event is always sent, kafka producer is always closed
    finally:
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "experiment_type": "resource_exhaustion",
            "parameters": {
                "cpu_cores": num_cores,
                "spec_duration": duration
            },
            "duration": (end_time - start_time).total_seconds()
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event):
            logger.warning(f"Failed to end start event to Kafka. See log for error.")

        kafka_prod.close()

if __name__ == "__main__":
    main()