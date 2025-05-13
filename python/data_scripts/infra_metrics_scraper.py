import os
import logging
import time
from datetime import datetime, timezone
import pickle

from kubernetes import client, config
from kubernetes.stream import stream

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InfraMetricsScraper:
    #Class for monitoring infra metrics during chaos experiments
    def __init__(self, scraper_id, target_pod_info, kube_config="~/.kube/config", scrape_interval=5):
        self.id = scraper_id
        self.target_pod_uid = target_pod_info['uid']
        self.target_pod_name = target_pod_info['name']
        self.target_pod_namespace = target_pod_info['namespace']
        self.target_node_name = target_pod_info['node']
        self.allocatable_cpu = None
        self.allocatable_memory = None
        self.kube_config = kube_config
        self.scrape_interval = scrape_interval
        self.experiment_detected = False
        self.kafka_prod = CapstoneKafkaProducer(topic='infra-metrics')
        self.run_loop = None
        self.start_time = None
        self.end_time = None
        self.message_sent_count = 0

        #K8s client setup
        if not self._k8s_client_setup():
            raise Exception(f"Unexpected error initializing Kubernetes client(s)")
        
        #Get allocatable CPU and memory
        if not self._resolve_target_node_info():
            raise Exception(f"Unexpected error parsing allocated CPU and memory")
        
    def __getstate__(self):
        state = self.__dict__.copy()
        #Remove unpickleable objects
        state['kafka_prod'] = None
        state['core_v1'] = None
        state['custom_obj_v1'] = None

        #Store kube config info to reconnect
        state['_kube_config'] = self.kube_config

        return state
    
    def __setstate__(self, state):
        self.__dict__.update(state)
        
        #Recreate the Kafka producer
        self.kafka_prod = CapstoneKafkaProducer(topic='infra-metrics')
        
        #Reestablish k8s client
        if not self._k8s_client_setup():
            raise Exception("Failed to reestablish Kubernetes client connection after unpickling")
        
    def set_experiment_detected(self, experiment_detected):
        self.experiment_detected = experiment_detected

    @staticmethod
    def save_instance(scraper_instance):
        with open('/tmp/infra_scraper.pickle', 'wb') as f:
            pickle.dump(scraper_instance, f)

    @staticmethod
    def get_instance():
        try:
            with open('/tmp/infra_scraper.pickle', 'rb') as f:
                import pickle
                return pickle.load(f)
        except (FileNotFoundError, pickle.UnpicklingError):
            return None
    
    def _k8s_client_setup(self):
        try:
            if self.kube_config:
                config.load_kube_config(config_file=self.kube_config)
            else:
                config.load_incluster_config()
            self.core_v1 = client.CoreV1Api()
            self.custom_obj_v1 = client.CustomObjectsApi()
            logger.info("Kubernetes clients initialized")
            return True
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
            return False

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
            'n': 1e-9,
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

    def _get_container_resource_limits(self):
        try:
            pod = self.core_v1.read_namespaced_pod(
                name=self.target_pod_name,
                namespace=self.target_pod_namespace
            )
            
            container_resources = {}
            
            if pod and pod.spec and pod.spec.containers:
                for container in pod.spec.containers:
                    container_name = container.name
                    resources = {}
                    
                    if container.resources:
                        #Get limits if available, otherwise use requests
                        if container.resources.limits:
                            cpu_limit = container.resources.limits.get('cpu')
                            memory_limit = container.resources.limits.get('memory')
                            
                            if cpu_limit:
                                resources['cpu_limit'] = self._parse_quantity(cpu_limit)
                            
                            if memory_limit:
                                resources['memory_limit'] = self._parse_quantity(memory_limit)
                        
                        #If no limits, try using requests
                        if container.resources.requests:
                            if not resources.get('cpu_limit') and container.resources.requests.get('cpu'):
                                resources['cpu_limit'] = self._parse_quantity(container.resources.requests.get('cpu'))
                            
                            if not resources.get('memory_limit') and container.resources.requests.get('memory'):
                                resources['memory_limit'] = self._parse_quantity(container.resources.requests.get('memory'))
                    
                    container_resources[container_name] = resources
            
            return container_resources
        except client.exceptions.ApiException as e:
            logger.warning(f"Failed to fetch pod resource limits: {e.reason}")
            return {}
        except Exception as e:
            logger.warning(f"Unexpected error fetching pod resource limits: {e}")
            return {}
    
    def _get_pod_metrics(self):
        if not self.custom_obj_v1:
            logger.error("CustomObjectsApi client is missing. Cannot fetch pod metrics.")
            return None
            
        try:
            pod_metrics = self.custom_obj_v1.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=self.target_pod_namespace,
                plural="pods",
                name=self.target_pod_name
            )
            
            container_resources = self._get_container_resource_limits()

            container_metrics = {}
            if pod_metrics and 'containers' in pod_metrics:
                for container in pod_metrics['containers']:
                    container_name = container.get('name')
                    if not container_name:
                        continue
                        
                    container_usage = container.get('usage', {})
                    cpu_usage_str = container_usage.get('cpu')
                    memory_usage_str = container_usage.get('memory')
                    
                    cpu_usage = None
                    cpu_percent = None
                    if cpu_usage_str:
                        try:
                            cpu_usage = self._parse_quantity(cpu_usage_str)
                            
                            #Calculate CPU percentage if limit is available
                            if container_name in container_resources and 'cpu_limit' in container_resources[container_name]:
                                cpu_limit = container_resources[container_name]['cpu_limit']
                                if cpu_limit > 0:
                                    cpu_percent = (cpu_usage / cpu_limit) * 100.0
                        except ValueError as e:
                            logger.error(f"Failed to parse CPU usage '{cpu_usage_str}' for container '{container_name}': {e}")
                    
                    memory_usage = None
                    memory_percent = None
                    if memory_usage_str:
                        try:
                            memory_usage = self._parse_quantity(memory_usage_str)
                            
                            #Calculate memory percentage if limit is available
                            if container_name in container_resources and 'memory_limit' in container_resources[container_name]:
                                memory_limit = container_resources[container_name]['memory_limit']
                                if memory_limit > 0:
                                    memory_percent = (memory_usage / memory_limit) * 100.0
                        except ValueError as e:
                            logger.error(f"Failed to parse memory usage '{memory_usage_str}' for container '{container_name}': {e}")
                    
                    container_metrics[container_name] = {
                        "cpu_usage": cpu_usage,
                        "cpu_percent": cpu_percent,
                        "memory_usage": memory_usage,
                        "memory_percent": memory_percent
                    }
                    
                logger.debug(f"Retrieved metrics for {len(container_metrics)} containers in pod {self.target_pod_name}")
                return container_metrics
            else:
                logger.warning(f"No container metrics found for pod {self.target_pod_namespace}/{self.target_pod_name}")
                return None
                
        except client.exceptions.ApiException as e:
            logger.warning(f"Failed to fetch metrics for pod '{self.target_pod_namespace}/{self.target_pod_name}': {e.reason}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error fetching metrics for pod '{self.target_pod_namespace}/{self.target_pod_name}': {e}")
            return None

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
        
        if not os.path.exists('/tmp/scraper_message_count.txt'):
            with open('/tmp/scraper_message_count.txt', 'w') as f:
                f.write('0')
        
        while self.run_loop: 
            time_scraped = datetime.now(timezone.utc)
            
            #Get node metrics
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
            
            #Get container metrics
            container_metrics = self._get_pod_metrics()
            
            log_message = f"Sending kafka event with:\nNode metrics: {cpu_usage}, {cpu_util_percent}, {memory_usage}, {memory_util_percent}"
            if container_metrics:
                container_logs = []
                for container_name, metrics in container_metrics.items():
                    container_logs.append(f"{container_name}: CPU={metrics['cpu_usage']}, CPU percent={metrics['cpu_percent']}, Memory={metrics['memory_usage']}, Memory percent={metrics['memory_percent']}")
                log_message += f"\nContainer metrics: {' | '.join(container_logs)}"
            logger.info(log_message)

            if container_metrics:
                logger.info(f"Including container metrics for {len(container_metrics)} containers")
            
            metrics_scrape = {
                "timestamp": time_scraped.isoformat(),
                "event_type": "monitor",
                "source": "infra_metrics_scraper",
                "experiment_detected": self.experiment_detected,
                "metrics": {
                    "node": {
                        "cpu_usage": {
                            "percent": cpu_util_percent,
                            "used": cpu_usage
                        },
                        "memory_usage": {
                            "percent": memory_util_percent,
                            "used": memory_usage
                        }
                    },
                    "containers": {}
                },  
                "parameters": {
                    "node": self.target_node_name,
                    "pod_uid": self.target_pod_uid,
                    "pod_name": self.target_pod_name,
                    "pod_namespace": self.target_pod_namespace
                }
            }
            
            #Add container metrics if available
            if container_metrics:
                for container_name, metrics in container_metrics.items():
                    metrics_scrape["metrics"]["containers"][container_name] = {
                        "cpu_usage": {
                            "percent": metrics["cpu_percent"],
                            "used": metrics["cpu_usage"]
                        },
                        "memory_usage": {
                            "percent": metrics["memory_percent"],
                            "used": metrics["memory_usage"]
                        }
                    }

            if not self.kafka_prod.send_event(metrics_scrape, self.id):
                logger.warning(f"Failed to send scraped INFRA METRICS to Kafka for node {self.target_node_name}. See producer log for error.")
            else:
                self.message_sent_count += 1
                #Store message count in file to be able to be retrieved by delete_all.py
                with open('/tmp/scraper_message_count.txt', 'w') as f:
                    f.write(str(self.message_sent_count))
            time.sleep(self.scrape_interval)
    
    def start(self):
        self.run_loop = True
        self.start_time = datetime.now(timezone.utc)

        #Store start time in file to be able to be retrieved by delete_all.py
        with open('/tmp/scraper_start_time.txt', 'w') as f:
            f.write(self.start_time.isoformat())

        self._monitor()

    def close(self):
        try:
            with open('/tmp/scraper_start_time.txt', 'r') as f:
                start_time_str = f.read().strip()
                self.start_time = datetime.fromisoformat(start_time_str)
        except:
            pass

        try:
            with open('/tmp/scraper_message_count.txt', 'r') as f:
                self.message_sent_count = int(f.read().strip())
        except:
            pass

        duration = None

        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        if duration:
            logger.info(f"Closing infra metrics scraper - logged metrics for {duration} seconds - sent {self.message_sent_count} messages to Kafka")
        else:
            logger.info(f"Closing infra metrics scraper - duration not calculated as start/end time incomplete.")
        self.run_loop = False
        self.kafka_prod.close()

        #Clean up tmp files
        for file_path in ['/tmp/scraper_start_time.txt', '/tmp/scraper_message_count.txt']:
            try:
                os.remove(file_path)
            except:
                pass

