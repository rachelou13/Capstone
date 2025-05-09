import logging
import time
from datetime import datetime, timezone

from kubernetes import client, config
from kubernetes.stream import stream

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InfraMetricsScraper:
    #Class for monitoring infra metrics during chaos experiments
    def __init__(self, experiment_id, target_pod_info, kube_config="~/.kube/config", scrape_interval=5):
        self.experiment_id = experiment_id
        self.target_pod_uid = target_pod_info['uid']
        self.target_pod_name = target_pod_info['name']
        self.target_pod_namespace = target_pod_info['namespace']
        self.target_node_name = target_pod_info['node']
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
                    "timestamp": datetime.now(timezone.utc).isoformat(), 
                    "event_type": "error",
                    "source": "infra_metrics_scraper", 
                    "error": f"K8s client init failed: {e}"
                }

                self.kafka_prod.send_event(k8s_fail_event, self.experiment_id)
            return
        
        #Get allocatable CPU and memory
        if not self._resolve_target_node_info():
            raise Exception(f"Unexpected error parsing allocated CPU and memory")

    @staticmethod
    def _parse_quantity(quantity_str: str) -> float:
        if not isinstance(quantity_str, str) or not quantity_str:
            raise ValueError(f"Quantity must be a non-empty string, got '{quantity_str}'")

        binary_suffixes = {
            'Ki': 2**10, 
            'Mi': 2**20, 
            'Gi': 2**30, 
            'Ti': 2**40, 
            'Pi': 2**50, 
            'Ei': 2**60
        }
        decimal_suffixes = {
            'm': 1e-3,   
            'k': 1e3,    
            'M': 1e6,    
            'G': 1e9,    
            'T': 1e12,   
            'P': 1e15,   
            'E': 1e18
        }

        numeric_part_str = quantity_str
        multiplier = 1.0
        processed_suffix = False

        if len(quantity_str) >= 3:
            suffix = quantity_str[-2:]
            if suffix in binary_suffixes:
                numeric_part_str = quantity_str[:-2]
                multiplier = binary_suffixes[suffix]
                processed_suffix = True
        
        if not processed_suffix and len(quantity_str) >= 2:
            suffix = quantity_str[-1:]
            if suffix in decimal_suffixes:
                numeric_part_str = quantity_str[:-1]
                multiplier = decimal_suffixes[suffix]
                processed_suffix = True
            elif not quantity_str[-1].isdigit():
                 raise ValueError(f"Unknown suffix or invalid format in quantity string: {quantity_str}")

        if not numeric_part_str:
            raise ValueError(f"No numeric value found in quantity string: {quantity_str}")

        try:
            value = float(numeric_part_str)
        except ValueError:
            raise ValueError(f"Invalid numeric part '{numeric_part_str}' in quantity string: {quantity_str}")
            
        return value * multiplier
    
    def _resolve_target_node_info(self):
        if not self.core_v1:
            logger.error("CoreV1Api not initialized. Cannot fetch node resources.")
            return False

        if not self.target_node_name:
            logger.error("Target node name is missing. Cannot fetch node allocatable resources.")
            return False
        try: 
            node = self.core_v1.read_node(self.target_node_name)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                logger.error(f"Node '{self.target_node_name}' not found.")
            else:
                logger.error(f"Kubernetes API error fetching node '{self.target_node_name}': Status {e.status}, Reason: {e.reason}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error fetching node '{self.target_node_name}': {e}")
            return False
        
        if not hasattr(node, 'status') or not node.status:
            logger.error(f"Node '{self.target_node_name}' has no status information.")
            return False
        
        if not hasattr(node.status, 'allocatable') or not node.status.allocatable:
            logger.error(f"Node '{self.target_node_name}' status has no 'allocatable' resources information.")
            return False
        
        allocatable_resources = node.status.allocatable
        
        cpu_quantity_str = allocatable_resources.get('cpu')
        memory_quantity_str = allocatable_resources.get('memory')

        #Parse CPU quantity from string
        success_cpu = False
        if cpu_quantity_str:
            try:
                self.allocatable_cpu = self._parse_quantity(cpu_quantity_str)
                success_cpu = True
            except ValueError as e:
                logger.error(f"Failed to parse CPU quantity '{cpu_quantity_str}' for node '{self.target_node_name}': {e}")
        else:
            logger.warning(f"Allocatable CPU quantity not found for node '{self.target_node_name}'.")

        #Parse memory quantity from string
        success_memory = False
        if memory_quantity_str:
            try:
                self.allocatable_memory = self._parse_quantity(memory_quantity_str)
                success_memory = True
            except ValueError as e:
                logger.error(f"Failed to parse Memory quantity '{memory_quantity_str}' for node '{self.target_node_name}': {e}")
        else:
            logger.warning(f"Allocatable Memory quantity not found for node '{self.target_node_name}'.")

        return success_cpu and success_memory
        
    def _monitor(self):
        if not self.target_node_name:
            logger.error("Target node name is missing. Monitoring cannot start.")
            self.run_loop = False
            return
        if self.allocatable_cpu is None or self.allocatable_memory is None:
            logger.error(f"Allocatable CPU or allocatable memory are missing. Monitoring cannot start.")
            self.run_loop = False
            return
        if not self.custom_obj_v1:
            logger.error("Kubernetes CustomObjectsApi client is missing. Monitoring cannot start.")
            self.run_loop = False
            return
            
        if not self.kafka_prod:
            logger.error("Kafka producer not is missing. Monitoring cannot start.")
            self.run_loop = False
            return
        
        while self.run_loop: 
            time_scraped = datetime.now(timezone.utc)
            node_metrics_data = None
            cpu_usage = None
            memory_usage = None
            try: 
                node_metrics_data = self.custom_obj_v1.get_cluster_custom_object(
                    group="metrics.k8s.io",
                    version="v1beta1",
                    plural="nodes",
                    name=self.target_node_name
                )
            except client.exceptions.ApiException as e:
                logger.warning(f"Failed to fetch metrics for node '{self.target_node_name}': {e.reason}. Retrying after interval.")
            except Exception as e:
                logger.warning(f"Unexpected error fetching metrics for node '{self.target_node_name}': {e}. Retrying after interval.")
            if node_metrics_data and 'usage' in node_metrics_data:
                usage = node_metrics_data['usage']
                cpu_usage_str = usage.get('cpu')
                memory_usage_str = usage.get('memory')

                if cpu_usage_str:
                    try:
                        cpu_usage = self._parse_quantity(cpu_usage_str)
                    except ValueError as e:
                        logger.error(f"Failed to parse CPU usage quantity '{cpu_usage_str}' for node '{self.target_node_name}': {e}")
                else:
                    logger.warning(f"CPU usage data not found in metrics for node '{self.target_node_name}'")
                
                if memory_usage_str:
                    try:
                        memory_usage = self._parse_quantity(memory_usage_str)
                    except ValueError as e:
                        logger.error(f"Failed to parse memory usage quantity '{memory_usage_str}' for node '{self.target_node_name}': {e}")
                else:
                    logger.warning(f"Memory usage data not found in metrics for node '{self.target_node_name}'")

            cpu_util_percent = None
            if cpu_usage is not None and self.allocatable_cpu > 0:
                cpu_util_percent = (cpu_usage / self.allocatable_cpu) * 100.0
            
            memory_util_percent = None
            if memory_usage is not None and self.allocatable_memory > 0:
                memory_util_percent = (memory_usage / self.allocatable_memory) * 100.0
            
            metrics_scrape = {
                "timestamp": time_scraped.isoformat(),
                "metrics": {
                    "cpu_usage": {
                        "percent": cpu_util_percent,
                        "used": cpu_usage
                    },
                    "memory_usage": {
                        "percent": memory_util_percent,
                        "used": memory_usage
                    }
                },  
                "parameters": {
                        "node": self.target_node_name,
                        "pod_uid": self.target_pod_uid,
                        "pod_name": self.target_pod_name,
                        "pod_namespace": self.target_pod_namespace
                    }
            }

            if not self.kafka_prod.send_event(metrics_scrape, self.experiment_id):
                logger.warning(f"Failed to send scraped INFRA METRICS to Kafka for node {self.target_node_name}. See producer log for error.")
            else:
                self.message_sent_count += 1
            time.sleep(self.scrape_interval)
    
    def start(self):
        self.run_loop = True
        self.start_time = datetime.now(timezone.utc)
        self._monitor()

    def close(self):
        self.end_time = datetime.now(timezone.utc)
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        if duration:
            logger.info(f"Closing infra metrics scraper - logged metrics for {duration} seconds - sent {self.message_sent_count} messages to Kafka")
        else:
            logger.info(f"Closing infra metrics scraper - duration not calculated as start/end time incomplete.")
        self.run_loop = False
        self.kafka_prod.close()

