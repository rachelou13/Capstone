import os
import logging
import argparse
import uuid
from datetime import datetime, timezone
import threading
import time

from kubernetes import client, config
from kubernetes.stream import stream
from kubernetes.client.rest import ApiException

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def apply_iptables_rule(api_client, pod_info, target_service, port):
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    core_v1 = client.CoreV1Api(api_client)
    
    setup_commands = [
        #Get the IP address of the target service
        f"TARGET_IP=$(getent hosts {target_service} | awk '{{print $1}}')",
        f"echo \"Blocking traffic to {target_service} ($TARGET_IP) port {port}\"",
        #Create iptables rule to drop packets
        f"iptables -I OUTPUT -d $TARGET_IP -p tcp --dport {port} -j DROP",
        #Verify rule was added
        f"iptables -L OUTPUT"
    ]
    
    command_str = " && ".join(setup_commands)
    
    try:
        logger.info(f"Applying iptables rule to block traffic to {target_service}:{port}")
        
        #Execute the command in the pod
        exec_command = ['sh', '-c', command_str]
        resp = stream(core_v1.connect_get_namespaced_pod_exec,
                     pod_name,
                     namespace,
                     command=exec_command,
                     stderr=True,
                     stdin=False,
                     stdout=True,
                     tty=False)
        
        logger.info(f"iptables command result: {resp}")
        
        #Check if command succeeded
        if "DROP" in resp and target_service in resp:
            logger.info(f"Successfully added iptables rule to block {target_service}")
            return True
        else:
            logger.warning(f"iptables rule might not have been added correctly")
            return False
            
    except Exception as e:
        logger.error(f"Failed to apply iptables rule: {e}")
        return False

def remove_iptables_rule(api_client, pod_info, target_service, port):
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    core_v1 = client.CoreV1Api(api_client)
    
    cleanup_commands = [
        #Get the IP address of the target service
        f"TARGET_IP=$(getent hosts {target_service} | awk '{{print $1}}')",
        f"echo \"Unblocking traffic to {target_service} ($TARGET_IP) port {port}\"",
        #Remove iptables rule
        f"iptables -D OUTPUT -d $TARGET_IP -p tcp --dport {port} -j DROP",
        #Verify rule was removed
        f"iptables -L OUTPUT"
    ]
    
    command_str = " && ".join(cleanup_commands)
    
    try:
        logger.info(f"Removing iptables rule to restore traffic to {target_service}:{port}")
        
        #Execute the command in the pod
        exec_command = ['sh', '-c', command_str]
        resp = stream(core_v1.connect_get_namespaced_pod_exec,
                     pod_name,
                     namespace,
                     command=exec_command,
                     stderr=True,
                     stdin=False,
                     stdout=True,
                     tty=False)
        
        logger.info(f"iptables cleanup result: {resp}")
        
        #Check if command succeeded
        if "DROP" not in resp or target_service not in resp:
            logger.info(f"Successfully removed iptables rule for {target_service}")
            return True
        else:
            logger.warning(f"iptables rule might not have been removed correctly")
            return False
            
    except Exception as e:
        logger.error(f"Failed to remove iptables rule: {e}")
        return False

def validate_connectivity(api_client, pod_info, target_service, port, expected_failure=False):
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    core_v1 = client.CoreV1Api(api_client)
        
    test_command = f"nc -zv -w 3 {target_service} {port} 2>&1 || echo 'CONNECTION_FAILED'"
    
    try:
        logger.info(f"Testing connectivity with netcat: {test_command}")
        
        exec_command = ['sh', '-c', test_command]
        resp = stream(core_v1.connect_get_namespaced_pod_exec,
                     pod_name,
                     namespace,
                     command=exec_command,
                     stderr=True,
                     stdin=False,
                     stdout=True,
                     tty=False)
        
        logger.info(f"Connection test result: {resp}")
        
        #Check results
        connection_failed = "CONNECTION_FAILED" in resp
        connection_succeeded = not connection_failed and (
            "succeeded" in resp.lower() or 
            "open" in resp.lower() or 
            "connected" in resp.lower()
        )
        
        if connection_succeeded:
            logger.info("Connection test succeeded")
            if expected_failure:
                logger.warning(f"Connection to {target_service}:{port} succeeded but should have failed")
                return False
            else:
                logger.info(f"Connection succeeded as expected")
                return True
        elif connection_failed:
            logger.info("Connection test failed")
            if not expected_failure:
                logger.warning(f"Connection to {target_service}:{port} failed but should have succeeded")
                return False
            else:
                logger.info(f"Connection failed as expected")
                return True
        else:
            logger.warning(f"Unclear result from connection test")
            return False
                
    except Exception as e:
        logger.error(f"Connection test failed with error: {e}")
        return False

def setup_timeout(api_client, pod_info, target_service, port, duration):
    def timeout_callback():
        logger.info(f"Timeout reached after {duration  + 60}s, removing iptables rule")
        try:
            remove_iptables_rule(api_client, pod_info, target_service, port)
        except Exception as e:
            logger.error(f"Error during automatic iptables rule removal: {e}")

    timer = threading.Timer(duration + 60, timeout_callback)
    timer.daemon = True
    timer.start()
    return timer

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
        default=30,
        metavar="SECONDS",
        help="Duration you want to maintain the partition for (in seconds)"
    )
    parser.add_argument(
        "-ts",
        "--target-service",
        type=str,
        required=False,
        default="mysql-primary",
        metavar="SERVICE_NAME",
        help="Service name to block traffic to"
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
    target_service = args.target_service
    port = args.port
    protocol = args.protocol
    kube_config = args.kube_config

    #Start kafka producer
    kafka_prod = CapstoneKafkaProducer()
    experiment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    #Initialize success trackers
    partition_successful = False
    rollback_successful = False
    partition_validated = None
    rollback_validated = None

    #K8s client setup
    try:
        if kube_config:
            config.load_kube_config(config_file=kube_config)
        else:
            config.load_incluster_config()
        api_client = client.ApiClient()
        core_v1 = client.CoreV1Api(api_client)
        logger.info("Kubernetes client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Kubernetes client: {e}")
        if kafka_prod and kafka_prod.connected:
            k8s_fail_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "source": "network_partition", 
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
                "source": "network_partition",
                "parameters": {
                    "pod_uid": pod_uid
                },
                "error": f"Pod discovery failed: {e}"
            }

            kafka_prod.send_event(pod_fail_event, experiment_id)
        return

    #Send start event to kafka
    start_event = {
    "timestamp": start_time.isoformat(),
    "experiment_id": experiment_id,
    "event_type": "start",
    "source": "network_partition",
    "parameters": {
            "node": target_pod_info.get('node'),
            "pod_uid": pod_uid,
            "pod_name": target_pod_info.get('name'),
            "pod_namespace": target_pod_info.get('namespace'),
            "target_service": target_service,
            "port": port,
            "protocol": protocol,
            "spec_duration": duration
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}. See producer log for error.")

    #Execute the experiment
    try:
        logger.info(f"Starting network partition experiment on pod {target_pod_info['namespace']}/{target_pod_info['name']} (UID: {pod_uid})")
        partition_successful = apply_iptables_rule(api_client, target_pod_info, target_service, port)

        if partition_successful:
            logger.info(f"Network partition successfully created with iptables rule")
            
            #Wait for rule to take effect
            time.sleep(3)
            
            #Validate partition was effective
            if target_service and port:
                logger.info(f"Validating connectivity to {target_service}")
                partition_validated = validate_connectivity(
                    api_client, target_pod_info, target_service, port, expected_failure=True
                )
                logger.info(f"Partition validation {'succeeded' if partition_validated else 'failed'}")

            #Set up timeout for automatic rule removal
            timer = setup_timeout(api_client, target_pod_info, target_service, port, duration)
            
            #Wait for specified duration
            logger.info(f"Maintaining network partition for {duration}s")
            time.sleep(duration)
            
            #Cancel timer if we reached here without timeout
            timer.cancel()
            
            #Remove iptables rule
            logger.info("Removing iptables rule to restore connectivity")
            rollback_successful = remove_iptables_rule(api_client, target_pod_info, target_service, port)
            
            #Wait for rule removal to take effect
            time.sleep(3)
            
            #Validate rollback was effective
            if target_service and port:
                logger.info(f"Validating connectivity to {target_service} after rollback")
                rollback_validated = validate_connectivity(
                    api_client, target_pod_info, target_service, port, expected_failure=False
                )
                logger.info(f"Rollback validation {'succeeded' if rollback_validated else 'failed'}")
        else:
            logger.error("Failed to create network partition")

    except Exception as e:
        logger.error(f"Unexpected error occurred while blocking network traffic: {e}")
        if kafka_prod and kafka_prod.connected:
            error_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "source": "network_partition",
                "parameters":
                    start_event["parameters"],
                "error": f"Network partition failed: {e}"
            }
            kafka_prod.send_event(error_event, experiment_id)

        #Attempt emergency rollback if partition was created
        if partition_successful:
            logger.info("Attempting emergency rollback by removing iptables rule")
            try:
                remove_iptables_rule(api_client, target_pod_info, target_service, port)
            except Exception as rollback_error:
                logger.error(f"Emergency rollback failed: {rollback_error}")
    
    #Ensure end event is always sent, kafka producer is always closed
    finally:
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "source": "network_partition",
            "parameters": start_event["parameters"],
            "success": partition_successful and rollback_successful,
            "details": {
                "partition_validated": partition_validated,
                "rollback_validated": rollback_validated,
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