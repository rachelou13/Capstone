import logging
import psutil
import time
from datetime import datetime, timezone
import socket

from kubernetes import client, config
from kubernetes.stream import stream

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InfraMetricsScraper:
    #Class for monitoring infra metrics during chaos experiments
    def __init__(self, experiment_id, target_pod_uid, kube_config="~/.kube/config", scrape_interval=5):
        self.experiment_id = experiment_id
        self.target_pod_uid = target_pod_uid
        self.target_node_name = None
        self.target_pod_name = None
        self.target_pod_namespace = None
        self.allocatable_cpu = None
        self.allocatable_memory = None
        self.kube_config = kube_config
        self.scrape_interval = scrape_interval
        self.kafka_prod = CapstoneKafkaProducer(topic='infra-metrics')
        self.run_loop = None
        self.start_time = None
        self.end_time = None
        self.message_sent_count = 0

        #K8s client setup
        try:
            if self.kube_config:
                config.load_kube_config(config_file=kube_config)
            else:
                config.load_incluster_config()
            self.core_v1 = client.CoreV1Api()
            self.custom_obj_v1 = client.CustomObjectsApi()
            logger.info("Kubernetes clients initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            if self.kafka_prod and self.kafka_prod.connected:
                k8s_fail_event = {
                    "timestamp": self.start_time.isoformat(), 
                    "event_type": "error",
                    "source": "infra_metrics_scraper", 
                    "error": f"K8s client init failed: {e}"
                }

                self.kafka_prod.send_event(k8s_fail_event, self.experiment_id)
            return
    
    def _resolve_target_info(self):
        

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

