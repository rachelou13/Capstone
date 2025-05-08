import logging
import psutil
import time
from datetime import datetime, timezone
import socket

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InfraMetricsScraper:
    #Class for monitoring infra metrics during chaos experiments
    def __init__(self, target_pod_uid, kube_config="~/.kube/config", scrape_interval=5):
        self.target_pod_uid = target_pod_uid
        self.scrape_interval = scrape_interval
        self.kafka_prod = CapstoneKafkaProducer(topic='infra-metrics')
        self.run_loop = True
        self.start_time = None
        self.end_time = None
        self.message_sent_count = 0
        self._monitor()


    def _monitor(self):
        self.start_time = datetime.now(timezone.utc)
        while self.run_loop: 
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

            if not self.kafka_prod.send_event(metrics_scrape):
                logger.warning(f"Failed to send scraped INFRA METRICS to Kafka. See producer log for error.")
            else:
                self.message_sent_count += 1
            time.sleep(self.scrape_interval)

    def close(self):
        self.end_time = datetime.now(timezone.utc)
        logger.info(f"Closing infra metrics scraper - logged metrics for {(self.end_time - self.start_time).total_seconds()} seconds - sent {self.message_sent_count} to Kafka")
        self.run_loop = False
        self.kafka_prod.close()

