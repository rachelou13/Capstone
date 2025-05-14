import os
import logging
import argparse
import uuid
from datetime import datetime, timezone
import threading
import time
import yaml


from kubernetes import client, config
from kubernetes.stream import stream
from kubernetes.client.rest import ApiException

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def remove_network_policy(api_client, namespace, policy_name):
    if not policy_name:
        logger.warning("No NetworkPolicy name provided for removal")
        return False
    
    try:
        networking_v1 = client.NetworkingV1Api(api_client)
        networking_v1.delete_namespaced_network_policy(name=policy_name, namespace=namespace)
        logger.info(f"Removed NetworkPolicy {policy_name} from namespace {namespace}")
        return True
    except ApiException as e:
        logger.error(f"Failed to remove NetworkPolicy {policy_name}: {e}")
        return False
    
def apply_network_policy(api_client, pod_info, target_service, direction):
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    policy_name = f"chaos-network-partition-{uuid.uuid4().hex[:8]}"
    
    #Get pod labels to use as selector
    core_v1 = client.CoreV1Api(api_client)
    pod = core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    pod_labels = pod.metadata.labels or {}
    
    if not pod_labels:
        logger.error(f"Pod {namespace}/{pod_name} has no labels. Cannot create NetworkPolicy.")
        return False, None
    
    #Try to find the target service to get its selectors
    try:
        #First try in the same namespace
        service = None
        try:
            service = core_v1.read_namespaced_service(
                name=target_service, 
                namespace=namespace
            )
            logger.info(f"Found service {target_service} in namespace {namespace}")
        except client.rest.ApiException as e:
            if e.status == 404:
                #If not found in pod's namespace, try in default namespace
                try:
                    service = core_v1.read_namespaced_service(
                        name=target_service, 
                        namespace="default"
                    )
                    logger.info(f"Found service {target_service} in namespace default")
                except client.rest.ApiException:
                    logger.error(f"Service {target_service} not found in either {namespace} or default namespace")
                    return False, None
            else:
                logger.error(f"Error reading service {target_service}: {e}")
                return False, None
        
        #Get service selector labels
        service_selector = service.spec.selector
        
        if not service_selector:
            logger.error(f"Service {target_service} doesn't have selector labels")
            return False, None
            
        logger.info(f"Service {target_service} has selector labels: {service_selector}")
        service_namespace = service.metadata.namespace
        
    except Exception as e:
        logger.error(f"Failed to find service info for {target_service}: {e}")
        return False, None
    
    #Create NetworkPolicy based on direction
    policy = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": policy_name,
            "namespace": namespace
        },
        "spec": {
            "podSelector": {
                "matchLabels": pod_labels
            },
            "policyTypes": []
        }
    }
    
    #Handle outbound traffic (pod to service)
    if direction in ["outbound", "both"]:
        policy["spec"]["policyTypes"].append("Egress")
        
        #Create a policy that allows all traffic EXCEPT to the target service
        policy["spec"]["egress"] = [
            #Allow DNS traffic
            {
                "ports": [
                    {"port": 53, "protocol": "UDP"}
                ]
            },
            #Allow all other traffic EXCEPT to the target service namespace
            {
                "to": [
                    {
                        "namespaceSelector": {
                            "matchExpressions": [
                                {
                                    "key": "kubernetes.io/metadata.name",
                                    "operator": "NotIn",
                                    "values": [service_namespace]
                                }
                            ]
                        }
                    }
                ]
            }
        ]
        
        #If the target service is in the same namespace as the pod
        if service_namespace == namespace:
            #Allow traffic to other pods in the same namespace
            policy["spec"]["egress"].append({
                "to": [
                    {
                        "podSelector": {
                            "matchExpressions": []
                        }
                    }
                ]
            })
            
            #For each label in the service selector, add a "NotIn" expression
            for key, value in service_selector.items():
                policy["spec"]["egress"][-1]["to"][0]["podSelector"]["matchExpressions"].append({
                    "key": key,
                    "operator": "NotIn",
                    "values": [value]
                })
    
    #Handle inbound traffic (service to pod)
    if direction in ["inbound", "both"]:
        policy["spec"]["policyTypes"].append("Ingress")
        
        #Create a policy that allows all traffic EXCEPT from the target service
        policy["spec"]["ingress"] = [
            #Allow all traffic EXCEPT from the target service namespace
            {
                "from": [
                    {
                        "namespaceSelector": {
                            "matchExpressions": [
                                {
                                    "key": "kubernetes.io/metadata.name",
                                    "operator": "NotIn",
                                    "values": [service_namespace]
                                }
                            ]
                        }
                    }
                ]
            }
        ]
        
        #If the target service is in the same namespace as the pod
        if service_namespace == namespace:
            #Allow traffic from other pods in the same namespace
            policy["spec"]["ingress"].append({
                "from": [
                    {
                        "podSelector": {
                            "matchExpressions": []
                        }
                    }
                ]
            })
            
            #For each label in the service selector, add a "NotIn" expression
            for key, value in service_selector.items():
                policy["spec"]["ingress"][-1]["from"][0]["podSelector"]["matchExpressions"].append({
                    "key": key,
                    "operator": "NotIn",
                    "values": [value]
                })
    
    #Log the full NetworkPolicy for debugging
    policy_yaml = yaml.dump(policy)
    logger.info(f"Creating NetworkPolicy:\n{policy_yaml}")
    
    # Apply the NetworkPolicy
    try:
        networking_v1 = client.NetworkingV1Api(api_client)
        networking_v1.create_namespaced_network_policy(namespace=namespace, body=policy)
        logger.info(f"Created NetworkPolicy {policy_name} in namespace {namespace}")
        return True, policy_name
    except ApiException as e:
        logger.error(f"Failed to create NetworkPolicy: {e}")
        return False, None

# def validate_connectivity(api_client, pod_info, target_service, port, expected_failure=False):
#     pod_name = pod_info['name']
#     namespace = pod_info['namespace']
#     core_v1 = client.CoreV1Api(api_client)
    
#     service_fqdn = f"{target_service}.{namespace}.svc.cluster.local"
    
#     test_commands = [
#         #Try netcat first
#         f"nc -zv -w 3 {service_fqdn} {port} 2>&1 || echo 'CONNECTION_FAILED'",
        
#         #Try curl as fallback
#         f"curl -s -m 3 -o /dev/null -w '%{{http_code}}' http://{service_fqdn}:{port} || echo 'CONNECTION_FAILED'",
        
#         #Try MySQL client if this is a MySQL connection
#         f"mysql -h {service_fqdn} -P {port} -e 'SELECT 1' 2>/dev/null && echo 'CONNECTION_SUCCESS' || echo 'CONNECTION_FAILED'",
        
#         #Try bash built-in as fallback
#         f"bash -c '(echo > /dev/tcp/{service_fqdn}/{port}) 2>/dev/null && echo CONNECTION_SUCCESS || echo CONNECTION_FAILED'",
        
#         #Final fallback for sh-only containers
#         f"(timeout 3 telnet {service_fqdn} {port} || echo 'CONNECTION_FAILED') 2>/dev/null"
#     ]
    
#     #Try each command until one works
#     for i, cmd in enumerate(test_commands):
#         try:
#             logger.info(f"Testing connectivity (attempt {i+1}/{len(test_commands)}): {cmd}")
            
#             exec_command = ['/bin/sh', '-c', cmd]
#             resp = stream(core_v1.connect_get_namespaced_pod_exec,
#                          pod_name,
#                          namespace,
#                          command=exec_command,
#                          stderr=True,
#                          stdin=False,
#                          stdout=True,
#                          tty=False)
            
#             logger.info(f"Connection test result: {resp}")
            
#             #Check if the command was not found
#             if "command not found" in resp or "not found" in resp:
#                 logger.warning(f"Command not available in container, trying next command")
#                 continue
                
#             #Check if connection succeeded or failed
#             connection_failed = "CONNECTION_FAILED" in resp
#             connection_succeeded = not connection_failed and (
#                 "CONNECTION_SUCCESS" in resp or 
#                 "open" in resp.lower() or 
#                 "connected" in resp.lower() or
#                 "established" in resp.lower() or
#                 "200" in resp
#             )
            
#             if connection_succeeded:
#                 logger.info(f"Connection test succeeded using command {i+1}")
#                 if expected_failure:
#                     logger.warning(f"Connection to {service_fqdn}:{port} succeeded but should have failed")
#                     return False
#                 else:
#                     logger.info(f"Connection succeeded as expected")
#                     return True
#             elif connection_failed:
#                 logger.info(f"Connection test failed using command {i+1}")
#                 if not expected_failure:
#                     logger.warning(f"Connection to {service_fqdn}:{port} failed but should have succeeded")
#                     return False
#                 else:
#                     logger.info(f"Connection failed as expected")
#                     return True
#             else:
#                 logger.warning(f"Unclear result from command {i+1}, trying next command")
#                 continue
                
#         except Exception as e:
#             logger.warning(f"Command {i+1} failed with error: {e}, trying next command")
#             continue
    
#     #ALL commands failed to run properly
#     logger.error("All connectivity test commands failed")
    
#     #Default to assuming the test passed if we expected failure
#     if expected_failure:
#         logger.info("Assuming connection is blocked as expected since all tests failed")
#         return True
#     else:
#         logger.warning("Assuming connection test failed since no method worked")
#         return False
    
def setup_timeout(api_client, target_pod_info, policy_name, duration):
    
    def timeout_callback():
        logger.info(f"Timeout reached after {duration}s, removing NetworkPolicy")
        try:
            remove_network_policy(api_client, target_pod_info['namespace'], policy_name)
        except Exception as e:
            logger.error(f"Error during automatic policy removal: {e}")

    timer = threading.Timer(duration, timeout_callback)
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
        default=15,
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
        "-dir",
        "--direction",
        type=str,
        required=False,
        default="outbound",
        choices=["outbound", "inbound", "both"],
        help="Direction of traffic to block (outbound, inbound, or both)"
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
    direction = args.direction
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
    policy_name = None

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
            "direction": direction,
            "spec_duration": duration
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}. See producer log for error.")

    #Execute the experiment
    try:
        logger.info(f"Starting network partition experiment on pod {target_pod_info['namespace']}/{target_pod_info['name']} (UID: {pod_uid})")
        partition_successful, policy_name = apply_network_policy(api_client, target_pod_info, target_service, direction)

        if partition_successful:
            logger.info(f"Network partition successfully created with policy {policy_name}")
            
            #Wait for policy to take effect
            time.sleep(3)
            
            #Validate partition was effective
            # if target_service and port:
            #     logger.info(f"Validating connectivity to {target_service}")
            #     partition_validated = validate_connectivity(
            #         api_client, target_pod_info, target_service, port, expected_failure=True
            #     )
            #     logger.info(f"Partition validation {'succeeded' if partition_validated else 'failed'}")
            
            #Set up timeout for automatic policy removal
            timer = setup_timeout(api_client, target_pod_info, policy_name, duration)
            
            #Wait for specified duration
            logger.info(f"Maintaining network partition for {duration}s")
            time.sleep(duration)
            
            #Cancel timer if we reached here without timeout
            timer.cancel()
            
            #Remove NetworkPolicy
            logger.info("Removing NetworkPolicy to restore connectivity")
            rollback_successful = remove_network_policy(api_client, target_pod_info['namespace'], policy_name)
            
            #Wait for policy removal
            time.sleep(3)
            
            # Validate rollback was effective (optional)
            # if target_service and port:
            #     logger.info(f"Validating connectivity to {target_service} after rollback")
            #     rollback_validated = validate_connectivity(
            #         api_client, target_pod_info, target_service, port, expected_failure=False
            #     )
            #     logger.info(f"Rollback validation {'succeeded' if rollback_validated else 'failed'}")
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

        #Attempt emergency rollback if policy was created
        if policy_name:
            logger.info("Attempting emergency rollback by removing NetworkPolicy")
            try:
                remove_network_policy(api_client, target_pod_info['namespace'], policy_name)
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
                "policy_name": policy_name
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