import logging
import psutil
import time
from data_processing_scripts.utils.kafka_producer import DataKafkaProducer
from datetime import datetime, timezone
import socket

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    kafka_prod = DataKafkaProducer()

    while True: 
        time_scraped = datetime.now(timezone.utc)
        metrics_scrape = {
            "timestamp": time_scraped.isoformat(),
            "metrics": {
                "cpu_utilization": psutil.cpu_percent(interval=1), 
                "memory_usage": {
                    "percent": psutil.virtual_memory().percent,
                    "used": psutil.virtual_memory().used
                },
                #How to target specific nodes?
                "labels": {
                    "node": socket.gethostname()
                }
            }
        }

        if not kafka_prod.send_event(metrics_scrape):
            logger.warning(f"Failed to send scraped metrics to Kafka. See log for error.")

        #Should we let user specify custom scraping interval?
        time.sleep(10)
    
    #How to exit loop?
    #Need to close kafka producer

if __name__ == "__main__":
    main()

