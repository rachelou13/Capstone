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

def exec_command_in_pod(api, pod_name, namespace, container_name, duration, command_list):
    try:
        logger.debug(f"Executing in {namespace}/{pod_name}/{container_name}: {command_list}")
        resp = stream(api.connect_get_namespaced_pod_exec,
                      pod_name,
                      namespace,
                      container=container_name,
                      command=command_list,
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
                      _request_timeout=duration + 60)

        stdout = resp.strip() if resp else ""
        logger.debug(f"Exec stdout for '{command_list[0]}':\n{stdout[:200]}...")
        return stdout, "", 0
    except client.exceptions.ApiException as e:
        logger.error(f"ApiException when executing command in {namespace}/{pod_name}/{container_name}: {e.reason} - {e.body}")
        return None, str(e.body), 1
    except Exception as e:
        logger.error(f"Unexpected error executing command in {namespace}/{pod_name}/{container_name}: {e}")
        return None, str(e), 1

def cpu_stress_in_pod(api, pod_info, container_names, num_cores, duration):
    cpu_stress_test_success = False
    pod_name = pod_info['name']
    namespace = pod_info['namespace']

    for container_name in container_names:
        background_pids_file = f"/tmp/chaos_cpu_pids_{container_name}.txt"
        
        command_string = (
            'echo "Script started at $(date)" > /tmp/cpu_stress_internal.log; '
            f"rm -f {background_pids_file}; "
            # Cleanup function
            "cleanup() { "
            f"  echo 'Cleaning up stress processes' >> /tmp/cpu_stress_internal.log; "
            f"  if [ -f {background_pids_file} ]; then "
            f"    echo 'PIDs to kill:' >> /tmp/cpu_stress_internal.log; "
            f"    cat {background_pids_file} >> /tmp/cpu_stress_internal.log; "
            f"    while read pid; do "
            f"      kill -9 $pid 2>/dev/null || echo \"Failed to kill PID $pid\" >> /tmp/cpu_stress_internal.log; "
            f"    done < {background_pids_file}; "
            f"    rm -f {background_pids_file}; "
            f"  fi; "
            "}; "
            "trap cleanup EXIT INT TERM; "
            f"for i in $(seq 1 {num_cores}); do "
            f'  (while true; do : ; done) & echo $! >> {background_pids_file}; '
            f"done; "
            f"echo 'CPU stress processes started, PIDs in {background_pids_file}'; "
            f'echo "Before sleep at $(date)" >> /tmp/cpu_stress_internal.log; '
            f"sleep {duration}; "
            f'echo "After sleep at $(date)" >> /tmp/cpu_stress_internal.log; '
            "cleanup; "
            f'echo "Script ended at $(date)" >> /tmp/cpu_stress_internal.log;'
        )
        
        cpu_stress_cmd_list = [
            '/bin/sh', '-c', command_string
        ]
        
        stdout, stderr, exit_code = exec_command_in_pod(api, pod_name, namespace, container_name, duration + 30, command_list=cpu_stress_cmd_list)
        
        if exit_code == 0:
            logger.info(f"CPU stress command completed in {namespace}/{pod_name}/{container_name}. Verifying cleanup...")
            
            # Check if PID file still exists
            verify_command = [
                '/bin/sh', 
                '-c', 
                f"[ -f {background_pids_file} ] && echo 'PID file still exists' || echo 'PID file removed'"
            ]
            verify_stdout, verify_stderr, verify_exit = exec_command_in_pod(api, pod_name, namespace, container_name, 10, command_list=verify_command)
            
            if verify_exit == 0 and "PID file removed" in verify_stdout:
                logger.info(f"Cleanup verified in {container_name} - PID file successfully removed")
                cpu_stress_test_success = True
            else:
                logger.warning(f"Cleanup may not be complete in {container_name}: {verify_stdout}")
                # Try one more cleanup
                final_cleanup = [
                    '/bin/sh',
                    '-c',
                    f"[ -f {background_pids_file} ] && xargs -r kill -9 < {background_pids_file} && rm -f {background_pids_file} || echo 'No PID file to clean'"
                ]
                exec_command_in_pod(api, pod_name, namespace, container_name, 10, command_list=final_cleanup)
                cpu_stress_test_success = True
        else:
            logger.error(f"CPU stress command failed in {namespace}/{pod_name}/{container_name}. Exit code: {exit_code}, Stderr: {stderr}")
            # Try emergency cleanup
            emergency_cleanup = [
                '/bin/sh',
                '-c',
                f"[ -f {background_pids_file} ] && xargs -r kill -9 < {background_pids_file} && rm -f {background_pids_file} || echo 'No PID file to clean'"
            ]
            exec_command_in_pod(api, pod_name, namespace, container_name, 10, command_list=emergency_cleanup)
            cpu_stress_test_success = False
            break
            
    return cpu_stress_test_success
    

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Stress test CPU by adding load to specified numbers of cores")
    
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
        help="Duration you want the CPU stress test to run for (in seconds)"
    )
    parser.add_argument(
        "-c",
        "--cores",
        type=int,
        required=False,
        default=2,
        metavar="CORES",
        help="Number of CPU stress processes to start in the target container (e.g., corresponding to virtual cores available to the pod/container)."
    )
    parser.add_argument(
        "-i",
        "--scrape-interval",
        required=False,
        default=5,
        type=int,
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
    num_cores = args.cores
    duration = args.duration
    scrape_interval = args.scrape_interval
    kube_config = args.kube_config

    #Start kafka producer
    kafka_prod = CapstoneKafkaProducer()
    experiment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    #Initialize scraper variables
    infra_scraper = None
    scraper_thread = None

    #Initialize success tracker
    cpu_stress_test_success = False

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
                "experiment_type": "resource_exhaustion", 
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
                "experiment_type": "resource_exhaustion",
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
        "experiment_type": "resource_exhaustion",
        "parameters": {
            "node": target_pod_info.get('node'),
            "pod_uid": pod_uid,
            "pod_name": target_pod_info.get('name'),
            "pod_namespace": target_pod_info.get('namespace'),
            "cpu_cores": num_cores,
            "spec_duration": duration
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}. See producer log for error.")

    #Execute experiment
    try:
        logger.info(f"Starting resource exhaustion experiment on pod {target_pod_info['namespace']}/{target_pod_info['name']} (UID: {pod_uid})")
        cpu_stress_test_success = cpu_stress_in_pod(core_v1, target_pod_info, target_container_names, num_cores, duration)
    except Exception as e:
        logger.error(f"Unexpected error occurred while running load on CPU(s): {e}")
        if kafka_prod and kafka_prod.connected:
            error_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "experiment_type": "resource_exhaust",
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
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "experiment_type": "resource_exhaustion",
            "parameters":
                    start_event["parameters"],
            "success": cpu_stress_test_success,
            "duration": (end_time - start_time).total_seconds()
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event, experiment_id):
                logger.warning(f"Failed to send END event to Kafka for experiment {experiment_id}. See producer log for error.")

        kafka_prod.close()
        logger.info(f"Experiment {experiment_id} finished. Duration: {(end_time - start_time).total_seconds():.2f}s.")

if __name__ == "__main__":
    main()