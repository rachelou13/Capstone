import logging
import argparse
import uuid
from datetime import datetime, timezone
import time

from kubernetes import client, config
from kubernetes.stream import stream

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
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
        logger.error(f"Unexpected error executing command in {namespace}/{pod_name}/{container_name}: {e}")
        return None, str(e), 1

def get_cgroup_info(api, pod_name, namespace, container_name, pid):
    logger.info(f"Attempting to retrieve cgroup information for PID {pid}")
    stdout, stderr, _ = exec_command_in_pod(api, pod_name, namespace, container_name, ['cat', f'/proc/{pid}/cgroup'])
    if stdout is not None:
        return stdout
    logger.warning(f"Failed to get cgroup information for PID {pid}. Stderr: {stderr}")
    return ""

def find_and_terminate_process(api, pod_info, container_names, pod_uid, container_id_prefix, process_pattern):
    process_terminated_count = 0
    pod_name = pod_info['name']
    namespace = pod_info['namespace']

    list_proc_cmd = [
        '/bin/sh', '-c',
        "find /proc -maxdepth 1 -type d -name '[0-9]*' | "
        "while IFS= read -r pid_dir; do "
        "  if [ -z \"$pid_dir\" ]; then continue; fi; "
        "  pid=$(basename \"$pid_dir\"); "
        "  name=$(cat \"$pid_dir/status\" 2>/dev/null | grep '^Name:' | cut -f2-); "
        "  cmdline_raw=$(cat \"$pid_dir/cmdline\" 2>/dev/null); "
        "  cmdline_spaces=$(echo \"$cmdline_raw\" | tr '\\0' ' '); "
        "  cmdline_trimmed=$(echo \"$cmdline_spaces\" | sed 's/[[:space:]]*$//'); "
        "  printf '%s\\t%s\\t%s\\n' \"$pid\" \"$name\" \"$cmdline_trimmed\"; "
        "done"
    ]
    

    for container_name in container_names:
        logger.info(f"Scanning container '{container_name}' in pod '{namespace}/{pod_name}'")
        stdout, stderr, _ = exec_command_in_pod(api, pod_name, namespace, container_name, list_proc_cmd)
        
        if stdout is None:
            logger.error(f"Failed to list processes in {namespace}/{pod_name}/{container_name}. Error: {stderr}")
            continue
        
        for line in stdout.strip().split('\n'):
            if not line:
                continue
            try:
                pid_str, p_name, p_cmdline = line.split('\t', 2)
                p_pid = int(pid_str)
            except ValueError:
                logger.warning(f"Could not parse process line: '{line}' in {container_name}")
                continue
            logger.debug(f"Checking PID {p_pid} (Name: {p_name}, Cmdline: {p_cmdline[:50]}...) in {container_name}")

            cgroup_info = get_cgroup_info(api, pod_name, namespace, container_name, p_pid)
            if not cgroup_info:
                logger.debug(f"Skipping PID {p_pid} in {container_name} - failed to get cgroup information.")
                continue
            
            pod_uid_found = False
            target_cgroup_line = None
            
            #Search for pod UID in cgroup information
            for line in cgroup_info.splitlines():
                if "kubepods" in line and pod_uid in line:
                    pod_uid_found = True
                    target_cgroup_line = line
                    break

            if not pod_uid_found:
                logger.debug(f"PID {p_pid} in {container_name} does not seem to belong to pod UID {pod_uid} based on its cgroup information - skipping")
                continue
            logger.debug(f"PID {p_pid} cgroup matches pod UID {pod_uid}")

            #Search for container prefix IF PROVIDED
            if container_id_prefix:
                if not target_cgroup_line or container_id_prefix not in target_cgroup_line:
                    logger.info(f"PID {p_pid} is in pod {pod_uid}, but not in container with prefix {container_id_prefix} - based on cgroup line {target_cgroup_line} - skipping")
                    continue
                logger.debug(f"PID {p_pid} cgroup line contains container ID prefix: '{container_id_prefix}'")

            #Search for process pattern IF PROVIDED
            if process_pattern:
                if not (process_pattern in p_cmdline or (p_name and process_pattern in p_name)):
                    logger.debug(f"PID {p_pid} ({p_name}) in {container_name} does not match process pattern: '{process_pattern}' - skipping")
                    continue
                logger.debug(f"PID {p_pid} ({p_name}) matches process pattern: '{process_pattern}'")

            logger.info(f"Target match: PID {p_pid} ({p_name}) in {namespace}/{pod_name}/{container_name} - attempting to terminate")

            #Send kill command
            kill_cmd = ['kill', str(p_pid)]
            _, stderr_kill, _ = exec_command_in_pod(api, pod_name, namespace, container_name, kill_cmd)
            if stderr_kill and "No such process" not in stderr_kill:
                logger.warning(f"Error sending SIGTERM to PID {p_pid} in {container_name}: {stderr_kill}")
            else:
                logger.info(f"Sent SIGTERM to PID {p_pid} ({p_name}) in {container_name}")
            
            #Wait for confirmation
            time.sleep(3)
            check_alive_cmd = ['ps', '-p', str(p_pid)]
            stdout_check, _, _ = exec_command_in_pod(api, pod_name, namespace, container_name, check_alive_cmd)
            if "PID" not in stdout_check:
                logger.info(f"PID {p_pid} confirmed terminated in {container_name}")
                process_terminated_count += 1
            else:
                logger.warning(f"PID {p_pid} in {container_name} did not terminate after 3s SIGTERM")
            
    return process_terminated_count

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Find and terminate processes in a Kubernetes pod")
    parser.add_argument(
        "-u",
        "--pod-uid",
        type=str,
        required=True,
        metavar="POD_UID",
        help="UID of the target Kubernetes pod"
    )
    parser.add_argument(
        "-c",
        "--container-id-prefix",
        type=str,
        required=False,
        default=None,
        metavar="CONTAINER_ID_PREFIX",
        help="Prefix of the target container ID. If the pod only has one container or you wish to target all containers in the pod matching the process pattern, omit this. This is a cgroup string match - not a container name."
    )
    parser.add_argument(
        "-p",
        "--process-pattern",
        type=str,
        required=False,
        default=None,
        metavar="PROCESS_PATTERN",
        help="String pattern to search for within the command line or name of processes running inside target pod"
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
    container_id_prefix = args.container_id_prefix
    process_pattern = args.process_pattern
    kube_config = args.kube_config

    #Start kafka producer
    kafka_prod = CapstoneKafkaProducer()
    experiment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    #Initialize counter
    process_terminated_count = 0

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
                "source": "terminate_process", 
                "error": f"K8s client init failed: {e}"
             }

            kafka_prod.send_event(k8s_fail_event, experiment_id)
        return

    #Find target pod and container info
    target_pod_info = None
    target_container_names = []

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
            logger.info(f"Scanning for processes in containers: {target_container_names}")
    except Exception as e:
        logger.error(f"Error during pod/container discovery for pod UID {pod_uid}: {e}")
        if kafka_prod and kafka_prod.connected:
            pod_fail_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "source": "terminate_process",
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
        "source": "terminate_process",
        "parameters": {
            "node": target_pod_info.get('node'),
            "pod_uid": pod_uid,
            "pod_name": target_pod_info.get('name'),
            "pod_namespace": target_pod_info.get('namespace'),
            "container_id_prefix": container_id_prefix,
            "process_pattern": process_pattern
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}. See producer log for error.")


    #Execute experiment
    try:
        logger.info(f"Starting process termination experiment on pod {target_pod_info['namespace']}/{target_pod_info['name']} (UID: {pod_uid}" + 
                    (f" - {container_id_prefix}" if container_id_prefix else "") + 
                    (f" - {process_pattern}" if process_pattern else "") + ")")
        process_terminated_count = find_and_terminate_process(core_v1, target_pod_info, target_container_names, pod_uid, container_id_prefix, process_pattern)
    except Exception as e:
        logger.error(f"An unexpected error occurred while terminating process(es): {e}")
        if kafka_prod and kafka_prod.connected:
            error_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "source": "terminate_process",
                "parameters":
                    start_event["parameters"],
                "error": f"Process termination failed: {e}"
            }
            kafka_prod.send_event(error_event, experiment_id)
    
    #Ensure end event is always sent, kafka producer is always closed
    finally:
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "source": "terminate_process",
            "parameters":
                start_event["parameters"],
            "success": process_terminated_count > 0,
            "details": {
                "process_terminated_count": process_terminated_count
            },
            "duration": duration
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event, experiment_id):
                logger.warning(f"Failed to send END event to Kafka for experiment {experiment_id}. See producer log for error.")

        kafka_prod.close()
        logger.info(f"Experiment {experiment_id} finished. Terminated {process_terminated_count} processes. Duration: {duration:.2f}s.")

if __name__ == "__main__":
    main()