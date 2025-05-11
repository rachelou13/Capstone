import logging
import argparse
import uuid
from datetime import datetime, timezone
import threading

from kubernetes import client, config
from kubernetes.stream import stream

from python.utils.kafka_producer import CapstoneKafkaProducer
from python.data_scripts.infra_metrics_scraper import InfraMetricsScraper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def exec_command_in_pod(api, pod_name, namespace, container_name, command_list):
    try:
        logger.debug(f"Executing in {namespace}/{pod_name}/{container_name}: {command_list}")
        resp = stream(api.connect_get_namespaced_pod_exec,
                      pod_name,
                      namespace,
                      container=container_name,
                      command=command_list,
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
                      _request_timeout=60)

        stdout = resp.strip() if resp else ""
        logger.debug(f"Exec stdout for '{command_list[0]}':\n{stdout[:200]}...")
        return stdout, "", 0
    except client.exceptions.ApiException as e:
        logger.error(f"ApiException when executing command in {namespace}/{pod_name}/{container_name}: {e.reason} - {e.body}")
        return None, str(e.body), 1
    except Exception as e:
        error_message = str(e)
        if "permission denied" in error_message.lower():
            logger.error(f"Permission denied when executing iptables command in {namespace}/{pod_name}/{container_name}. Container may need privileged mode.")
        else:
            logger.error(f"Unexpected error executing command in {namespace}/{pod_name}/{container_name}: {e}")
        return None, error_message, 1

def main():
     #Parse args from command line
    parser = argparse.ArgumentParser(description="Create network partitions to block traffic between Kubernetes pods")
    parser.add_argument(
        "-u",
        "--pod-uid",
        type=str,
        required=True,
        metavar="POD_UID",
        help="UID of the target Kubernetes pod"
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        required=False,
        default=15,
        metavar="SECONDS",
        help="Duration you want to maintain the partition for (in seconds)"
    )
    parser.add_argument(
        "-t",
        "--target-host",
        type=str,
        required=False,
        default="mysql-primary",
        metavar="HOST_NAME",
        help="Host name to block traffic to"
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        required=False,
        default=3306,
        metavar="PORT_NUMBER",
        help="Port to block traffic on"
    )
    parser.add_argument(
        "-pr",
        "--protocol",
        type=str,
        required=False,
        default="tcp",
        choices=["tcp", "udp", "icmp"],
        help="Protocol to block (tcp, udp, or icmp)"
    )
    parser.add_argument(
        "-dir",
        "--direction",
        type=str,
        required=False,
        default="outbound",
        choices=["outbound", "inbound", "both"],
        help="Direction of traffic to block (outbound, inbound, or both)"
    )
    parser.add_argument(
        "-i",
        "--scrape-interval",
        type=int,
        required=False,
        default=5,
        metavar="SCRAPE_INTERVAL",
        help="How often (in seconds) to scrape metrics"
    )
    parser.add_argument(
        "-k",
        "--kube-config",
        type=str,
        required=False,
        default="~/.kube/config",
        metavar="KUBE_CONFIG",
        help="Path to kubeconfig file (if not running in-cluster)"
    )

    args = parser.parse_args()
    pod_uid = args.pod_uid
    duration = args.duration
    target_host = args.target_host
    port = args.port
    protocol = args.protocol
    direction = args.direction
    scrape_interval = args.scrape_interval
    kube_config = args.kube_config

    #Start kafka producer
    kafka_prod = CapstoneKafkaProducer()
    experiment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    #Initialize scraper variables
    infra_scraper = None
    scraper_thread = None

    #Initialize success trackers
    partition_successful = False
    rollback_successful = False
    partition_validated = None
    rollback_validated = None
    rules_applied = 0

    #K8s client setup
    try:
        if kube_config:
            config.load_kube_config(config_file=kube_config)
        else:
            config.load_incluster_config()
        core_v1 = client.CoreV1Api()
        logger.info("Kubernetes client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Kubernetes client: {e}")
        if kafka_prod and kafka_prod.connected:
            k8s_fail_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "experiment_type": "network_partition", 
                "error": f"K8s client init failed: {e}"
             }

            kafka_prod.send_event(k8s_fail_event, experiment_id)
        return
    
    #Find target pod and container info
    try:
        logger.debug(f"Searching for pod with UID: {pod_uid}")
        all_pods = core_v1.list_pod_for_all_namespaces(watch=False, timeout_seconds=60)
        found_pod = None
        for pod in all_pods.items:
            if pod.metadata.uid == pod_uid:
                target_pod_info = {
                    'uid': pod.metadata.uid,
                    'name': pod.metadata.name,
                    'namespace': pod.metadata.namespace,
                    'node': pod.spec.node_name
                }

                found_pod = pod
                logger.info(f"Found pod matching pod UID {pod_uid}: {target_pod_info['namespace']}/{target_pod_info['name']}")
                break
        
        if not found_pod:
            logger.error(f"Pod UID {pod_uid} not found in cluster")
            raise ValueError(f"Pod UID {pod_uid} not found in cluster")
        
        if found_pod.spec.containers:
            target_container_names = [c.name for c in found_pod.spec.containers]
            if not target_container_names:
                logger.error(f"No containers found in spec for pod {target_pod_info['namespace']}/{target_pod_info['name']}")
                raise ValueError("No containers in pod spec")
            logger.info(f"Found containers: {target_container_names} in pod UID {pod_uid} ({target_pod_info['name']})")
    except Exception as e:
        logger.error(f"Error during pod/container discovery for pod UID {pod_uid}: {e}")
        if kafka_prod and kafka_prod.connected:
            pod_fail_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "experiment_type": "network_partition",
                "parameters": {
                    "pod_uid": pod_uid
                },
                "error": f"Pod discovery failed: {e}"
            }

            kafka_prod.send_event(pod_fail_event, experiment_id)
        return
    
    #Start scraper for infra metrics
    try:
        infra_scraper = InfraMetricsScraper(experiment_id=experiment_id, target_pod_info=target_pod_info, kube_config=kube_config, scrape_interval=scrape_interval)
        scraper_thread = threading.Thread(target=infra_scraper.start, daemon=True)
        scraper_thread.start()
        logger.info(f"InfraMetricsScraper initialized for pod UID {pod_uid}")
    except Exception as e:
        logger.error(f"Failed to initialize or start InfraMetricsScraper: {e}")
        infra_scraper = None
        scraper_thread = None

    #Send start event to kafka
    start_event = {
    "timestamp": start_time.isoformat(),
    "experiment_id": experiment_id,
    "event_type": "start",
    "experiment_type": "network_partition",
    "parameters": {
            "node": target_pod_info.get('node'),
            "pod_uid": pod_uid,
            "pod_name": target_pod_info.get('name'),
            "pod_namespace": target_pod_info.get('namespace'),
            "target_host": target_host,
            "port": port,
            "protocol": protocol,
            "direction": direction,
            "spec_duration": duration
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}. See producer log for error.")

    #Execute the experiment
    try:
        #Put call to apply_iptables_rules here
        pass
    except Exception as e:
        logger.error(f"Unexpected error occurred while blocking network traffic: {e}")
        if kafka_prod and kafka_prod.connected:
            error_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "experiment_type": "network_partition",
                "parameters":
                    start_event["parameters"],
                "error": f"Process termination failed: {e}"
            }
            kafka_prod.send_event(error_event, experiment_id)
    
    #Ensure end event is always sent, kafka producer is always closed
    finally:
        #Close the scraper for infra metrics
        if infra_scraper:
            infra_scraper.close()
            if scraper_thread and scraper_thread.is_alive():
                logger.info("Waiting for scraper thread to finish...")
                scraper_thread.join(timeout=10) 
                if scraper_thread.is_alive():
                    logger.warning("Scraper thread did not finish in time")

        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "experiment_type": "network_partition",
            "parameters": start_event["parameters"],
            "success": partition_successful and rollback_successful,
            "details": {
                "partition_validated": partition_validated,
                "rollback_validated": rollback_validated,
                "rules_applied": rules_applied
            },
            "duration": duration
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event, experiment_id):
                logger.warning(f"Failed to send END event to Kafka for experiment {experiment_id}. See producer log for error.")

        kafka_prod.close()
        logger.info(f"Experiment {experiment_id} finished. Duration: {duration:.2f}s.")

if __name__ == "__main__":
    main()