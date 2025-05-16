import logging
import argparse
import uuid
from datetime import datetime, timezone

from kubernetes import client, config
from kubernetes.stream import stream

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def exec_command_in_pod(api_client, pod_name, namespace, container_name, duration, command_list):
    try:
        core_v1 = client.CoreV1Api(api_client)
        logger.debug(f"Executing in {namespace}/{pod_name}/{container_name}: {command_list}")
        resp = stream(core_v1.connect_get_namespaced_pod_exec,
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

def parse_quantity(quantity_str):
        if not isinstance(quantity_str, str) or not quantity_str:
            raise ValueError(f"Quantity must be a non-empty string, got '{quantity_str}'")

        binary_suffixes = {
            'Ki': 2**10, 'Mi': 2**20, 'Gi': 2**30, 'Ti': 2**40, 'Pi': 2**50, 'Ei': 2**60
        }
        decimal_suffixes = {
            'n': 1e-9, 'm': 1e-3, 'k': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'E': 1e18
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

def get_memory_allocation(api_client, pod_info):
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    node_name = pod_info.get('node')
    
    try:
        core_v1 = client.CoreV1Api(api_client)
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        
        #Get total memory allocation for the pod
        total_memory_limit = 0
        for container in pod.spec.containers:
            if container.resources and container.resources.limits and 'memory' in container.resources.limits:
                memory_limit = container.resources.limits['memory']
                try:
                    total_memory_limit += parse_quantity(memory_limit)
                except ValueError as e:
                    logger.warning(f"Could not parse memory limit '{memory_limit}': {e}")
        
        #If no memory limit is set, try to get node allocatable memory
        if total_memory_limit == 0 and node_name:
            logger.info(f"No memory limit found for pod {namespace}/{pod_name}, trying to get node allocatable memory")
            try:
                node = core_v1.read_node(node_name)
                if node.status and node.status.allocatable and 'memory' in node.status.allocatable:
                    node_memory_str = node.status.allocatable['memory']
                    total_memory_limit = parse_quantity(node_memory_str)
                    logger.info(f"Using node allocatable memory: {total_memory_limit} bytes")
                else:
                    logger.warning(f"Node allocatable memory info not found for node {node_name}")
                    total_memory_limit = 100 * 1024 * 1024
            except Exception as e:
                logger.warning(f"Could not get node allocatable memory: {e}")
                total_memory_limit = 100 * 1024 * 1024
        
        #Default if still no memory limit
        if total_memory_limit == 0:
            total_memory_limit = 100 * 1024 * 1024
            logger.info(f"Defaulting to 100MB for memory intensity calculation")
        
        return total_memory_limit
    except Exception as e:
        logger.warning(f"Could not determine memory allocation for pod {namespace}/{pod_name}: {e}")
        logger.warning("Defaulting to 100MB")
        return 100 * 1024 * 1024

def memory_stress_in_pod(api_client, pod_info, container_names, intensity, duration):
    memory_stress_test_success = False
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    
    total_memory_limit = get_memory_allocation(api_client, pod_info)
    
    scaling_factor = 1.5  #Allocate 50% more than requested to account for OS caching
    adjusted_intensity = min(intensity * scaling_factor, 95)  #Cap at 95% to avoid OOM killer
    
    memory_to_allocate_mb = int((total_memory_limit * (adjusted_intensity / 100)) / (1024 * 1024))
    memory_to_allocate_mb = max(10, memory_to_allocate_mb)  #Minimum 10MB to ensure we create some pressure
    
    logger.info(f"Memory limit: {total_memory_limit / (1024*1024):.2f}MB, requested intensity: {intensity}%, "
                f"adjusted intensity: {adjusted_intensity}%, allocating {memory_to_allocate_mb}MB")

    for container_name in container_names:
        background_pids_file = f"/tmp/chaos_memory_pids_{container_name}.txt"
        memory_file = f"/tmp/memory-file-{container_name}"
        
        command_string = (
            'echo "Memory stress started at $(date)" > /tmp/memory_stress_internal.log; '
            f"rm -f {background_pids_file}; "
            f"mkdir -p /tmp/mem_stress_chunks; "
            
            #Record memory before stress
            'echo "Memory BEFORE stress:" >> /tmp/memory_stress_internal.log; '
            'cat /proc/meminfo | grep -E "MemTotal|MemFree|MemAvailable|Cached|Buffers" >> /tmp/memory_stress_internal.log; '
            
            #Cleanup function
            "cleanup() { "
            '  echo "Cleaning up memory stress files and processes" >> /tmp/memory_stress_internal.log; '
            f"  if [ -f {background_pids_file} ]; then "
            f"    echo 'PIDs to kill:' >> /tmp/memory_stress_internal.log; "
            f"    cat {background_pids_file} >> /tmp/memory_stress_internal.log; "
            f"    while read pid; do "
            f"      kill -9 $pid 2>/dev/null || echo \"Failed to kill PID $pid\" >> /tmp/memory_stress_internal.log; "
            f"    done < {background_pids_file}; "
            f"    rm -f {background_pids_file}; "
            f"  fi; "
            "  rm -rf /tmp/mem_stress_chunks; "
            "  sync; "
            "}; "
            "trap cleanup EXIT INT TERM; "
            
            #Calculate chunk size - use smaller chunks for better memory pressure
            "CHUNK_SIZE_MB=32; "  #Using 32MB chunks
            f"NUM_CHUNKS=$(({memory_to_allocate_mb} / $CHUNK_SIZE_MB + 1)); "
            f"LAST_CHUNK_SIZE=$(({memory_to_allocate_mb} % $CHUNK_SIZE_MB)); "
            
            #Memory allocation loop
            "for i in $(seq 1 $NUM_CHUNKS); do "
            "  if [ $i -eq $NUM_CHUNKS ] && [ $LAST_CHUNK_SIZE -gt 0 ]; then "
            "    CURR_CHUNK_SIZE=$LAST_CHUNK_SIZE; "
            "  else "
            "    CURR_CHUNK_SIZE=$CHUNK_SIZE_MB; "
            "  fi; "
            "  if [ $CURR_CHUNK_SIZE -eq 0 ]; then continue; fi; "
            
            #Use dd with urandom to create incompressible data that must stay in memory
            "  (dd if=/dev/urandom of=/tmp/mem_stress_chunks/chunk_$i bs=1M count=$CURR_CHUNK_SIZE status=none || true) & "
            "  echo $! >> {background_pids_file}; "
            "done; "
            
            #Wait for allocation to complete
            "sleep 2; "
            
            #Start a background process that periodically touches memory to keep it resident
            "("
            "  while true; do "
            #Read a small amount from each file to keep it active in memory
            "    find /tmp/mem_stress_chunks -type f -name 'chunk_*' | xargs -I{} dd if={} of=/dev/null bs=4k count=1 status=none 2>/dev/null || true; "
            "    sleep 5; "  #Touch every 5 seconds
            "  done "
            ") & echo $! >> {background_pids_file}; "
            
            #Force filesystem sync to ensure all writes hit storage/memory
            "sync; "
            
            #Check memory after allocation
            'echo "Memory AFTER allocation:" >> /tmp/memory_stress_internal.log; '
            'cat /proc/meminfo | grep -E "MemTotal|MemFree|MemAvailable|Cached|Buffers" >> /tmp/memory_stress_internal.log; '
            
            #Print success message
            f'echo "Memory stress processes started with {memory_to_allocate_mb}MB, PIDS in {background_pids_file}" >> /tmp/memory_stress_internal.log; '
            
            #Monitor memory during stress at intervals
            "for i in $(seq 1 5); do "
            f"  sleep $(({duration} / 5)); "
            "  echo \"Memory check $i:\" >> /tmp/memory_stress_internal.log; "
            "  cat /proc/meminfo | grep -E 'MemTotal|MemFree|MemAvailable|Cached|Buffers' >> /tmp/memory_stress_internal.log; "
            "done; "
            
            #Sleep for the remaining time
            f"REMAINING_SLEEP=$(({duration} - ({duration} / 5) * 5)); "
            "if [ $REMAINING_SLEEP -gt 0 ]; then sleep $REMAINING_SLEEP; fi; "
            
            'echo "Memory BEFORE cleanup:" >> /tmp/memory_stress_internal.log; '
            'cat /proc/meminfo | grep -E "MemTotal|MemFree|MemAvailable|Cached|Buffers" >> /tmp/memory_stress_internal.log; '
            
            #Cleanup
            'echo "Stress test complete" >> /tmp/memory_stress_internal.log; '
        )
        
        memory_stress_cmd_list = [
            'sh', '-c', command_string
        ]
        
        #Increase the timeout to account for the file operations
        stdout, stderr, exit_code = exec_command_in_pod(api_client, pod_name, namespace, container_name, duration + 120, command_list=memory_stress_cmd_list)
        
        if exit_code == 0:
            logger.info(f"Memory stress command completed in {namespace}/{pod_name}/{container_name}. Verifying cleanup...")
            
            #Check if PID file still exists
            verify_command = [
                'sh', 
                '-c', 
                f"[ -f {background_pids_file} ] && echo 'PID file still exists' || echo 'PID file removed'"
            ]
            verify_stdout, verify_stderr, verify_exit = exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=verify_command)
            
            #Check if memory directory still exists
            verify_dir_command = [
                'sh',
                '-c',
                "[ -d /tmp/mem_stress_chunks ] && echo 'Memory chunks directory still exists' || echo 'Memory chunks directory removed'"
            ]
            verify_dir_stdout, verify_dir_stderr, verify_dir_exit = exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=verify_dir_command)
            
            #Check memory usage statistics to verify effect
            memory_check = [
                'sh',
                '-c',
                'cat /proc/meminfo | grep -E "MemTotal|MemFree|MemAvailable"'
            ]
            memory_stdout, memory_stderr, memory_exit = exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=memory_check)
            logger.info(f"Memory stats after test: {memory_stdout}")
            
            if verify_exit == 0 and "PID file removed" in verify_stdout and "Memory chunks directory removed" in verify_dir_stdout:
                logger.info(f"Cleanup verified in {container_name} - memory stress resources successfully removed")
                memory_stress_test_success = True
            else:
                logger.warning(f"Cleanup may not be complete in {container_name}. PID check: {verify_stdout}, Directory check: {verify_dir_stdout}")
                #Try one more cleanup
                final_cleanup = [
                    'sh',
                    '-c',
                    f"[ -f {background_pids_file} ] && xargs -r kill -9 < {background_pids_file} && rm -f {background_pids_file} || echo 'No PID file to clean'; rm -rf /tmp/mem_stress_chunks"
                ]
                exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=final_cleanup)
                memory_stress_test_success = True
        else:
            logger.error(f"Memory stress command failed in {namespace}/{pod_name}/{container_name}. Exit code: {exit_code}, Stderr: {stderr}")
            logger.error(f"Command output: {stdout}")
            #Try emergency cleanup
            emergency_cleanup = [
                'sh',
                '-c',
                f"[ -f {background_pids_file} ] && xargs -r kill -9 < {background_pids_file} && rm -f {background_pids_file} || echo 'No PID file to clean'; rm -rf /tmp/mem_stress_chunks"
            ]
            exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=emergency_cleanup)
            memory_stress_test_success = False
            break
            
    return memory_stress_test_success

def get_cpu_allocation(api_client, pod_info):
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    node_name = pod_info.get('node')
    
    try:
        core_v1 = client.CoreV1Api(api_client)
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        
        #Get total CPU allocation for the pod
        total_cpu_limit = 0
        for container in pod.spec.containers:
            if container.resources and container.resources.limits and 'cpu' in container.resources.limits:
                cpu_limit = container.resources.limits['cpu']
                try:
                    #Convert CPU limit to numeric value
                    total_cpu_limit += parse_quantity(cpu_limit)
                except ValueError as e:
                    logger.warning(f"Could not parse CPU limit '{cpu_limit}': {e}")
        
        #If no CPU limit is set, try to get node allocatable CPU
        if total_cpu_limit == 0 and node_name:
            logger.info(f"No CPU limit found for pod {namespace}/{pod_name}, trying to get node allocatable CPU")
            try:
                node = core_v1.read_node(node_name)
                if node.status and node.status.allocatable and 'cpu' in node.status.allocatable:
                    node_cpu_str = node.status.allocatable['cpu']
                    total_cpu_limit = parse_quantity(node_cpu_str)
                    logger.info(f"Using node allocatable CPU: {total_cpu_limit} cores")
                else:
                    logger.warning(f"Node allocatable CPU info not found for node {node_name}")
                    total_cpu_limit = 1
            except Exception as e:
                logger.warning(f"Could not get node allocatable CPU: {e}")
                total_cpu_limit = 1
        
        #Default to 2 cores if still no CPU limit
        if total_cpu_limit == 0:
            total_cpu_limit = 2
            logger.info(f"Defaulting to 2 cores for intensity calculation")
        
        return total_cpu_limit
    except Exception as e:
        logger.warning(f"Could not determine CPU allocation for pod {namespace}/{pod_name}: {e}")
        logger.warning("Defaulting to 2 cores")
        return 2

def cpu_stress_in_pod(api_client, pod_info, container_names, intensity, duration):
    cpu_stress_test_success = False
    pod_name = pod_info['name']
    namespace = pod_info['namespace']
    node_name = pod_info.get('node')

    total_cpu_limit = get_cpu_allocation(api_client, pod_info)
    
    #Calculate number of cores based on intensity percentage
    num_cores = max(1, int(round(total_cpu_limit * (intensity / 100))))
    logger.info(f"CPU limit: {total_cpu_limit} cores, intensity: {intensity}%, using {num_cores} processes")

    for container_name in container_names:
        background_pids_file = f"/tmp/chaos_cpu_pids_{container_name}.txt"
        
        command_string = (
            'echo "Script started at $(date)" > /tmp/cpu_stress_internal.log; '
            f"rm -f {background_pids_file}; "
            #Cleanup function
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
            'sh', '-c', command_string
        ]
        
        stdout, stderr, exit_code = exec_command_in_pod(api_client, pod_name, namespace, container_name, duration + 30, command_list=cpu_stress_cmd_list)
        
        if exit_code == 0:
            logger.info(f"CPU stress command completed in {namespace}/{pod_name}/{container_name}. Verifying cleanup...")
            
            #Check if PID file still exists
            verify_command = [
                'sh', 
                '-c', 
                f"[ -f {background_pids_file} ] && echo 'PID file still exists' || echo 'PID file removed'"
            ]
            verify_stdout, verify_stderr, verify_exit = exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=verify_command)
            
            if verify_exit == 0 and "PID file removed" in verify_stdout:
                logger.info(f"Cleanup verified in {container_name} - PID file successfully removed")
                cpu_stress_test_success = True
            else:
                logger.warning(f"Cleanup may not be complete in {container_name}: {verify_stdout}")
                #Try one more cleanup
                final_cleanup = [
                    'sh',
                    '-c',
                    f"[ -f {background_pids_file} ] && xargs -r kill -9 < {background_pids_file} && rm -f {background_pids_file} || echo 'No PID file to clean'"
                ]
                exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=final_cleanup)
                cpu_stress_test_success = True
        else:
            logger.error(f"CPU stress command failed in {namespace}/{pod_name}/{container_name}. Exit code: {exit_code}, Stderr: {stderr}")
            #Try emergency cleanup
            emergency_cleanup = [
                'sh',
                '-c',
                f"[ -f {background_pids_file} ] && xargs -r kill -9 < {background_pids_file} && rm -f {background_pids_file} || echo 'No PID file to clean'"
            ]
            exec_command_in_pod(api_client, pod_name, namespace, container_name, 10, command_list=emergency_cleanup)
            cpu_stress_test_success = False
            break
            
    return cpu_stress_test_success
    

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Stress test CPU and/or memory resources with configurable intensity")
    
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
        help="Duration you want the stress test to run for (in seconds)"
    )
    parser.add_argument(
        "-cc",
        "--cpu-exhaust",
        action="store_true",
        help="Enable CPU exhaustion"
    )
    parser.add_argument(
        "-ci",
        "--cpu-intensity",
        type=int,
        required=False,
        default=100,
        metavar="PERCENTAGE",
        help="CPU intensity percentage (1-100)"
    )
    parser.add_argument(
        "-mc",
        "--memory-exhaust",
        action="store_true",
        help="Enable memory exhaustion"
    )
    parser.add_argument(
        "-mi",
        "--memory-intensity", 
        type=int,
        required=False,
        default=80,
        metavar="PERCENTAGE",
        help="Memory intensity percentage (1-100). Default is 80% to avoid OOM killer."
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
    kube_config = args.kube_config

    cpu_intensity = args.cpu_intensity
    memory_intensity = args.memory_intensity

    #Validate intensity percentages
    if args.cpu_exhaust and (cpu_intensity <= 0 or cpu_intensity > 100):
        logger.error(f"Invalid CPU intensity value: {cpu_intensity}. Must be between 1 and 100.")
        return
    
    if args.memory_exhaust and (memory_intensity <= 0 or memory_intensity > 100):
        logger.error(f"Invalid memory intensity value: {memory_intensity}. Must be between 1 and 100.")
        return

    #Start kafka producer
    kafka_prod = CapstoneKafkaProducer()
    experiment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    #Initialize success tracker
    cpu_stress_test_success = False

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
                "source": "resource_exhaustion", 
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
                logger.info(f"Found pod matching pod UID {pod_uid}: {target_pod_info['namespace']}/{target_pod_info['name']} on node {target_pod_info['node']}")
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
                "source": "resource_exhaustion",
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
        "source": "resource_exhaustion",
        "parameters": {
            "node": target_pod_info.get('node'),
            "pod_uid": pod_uid,
            "pod_name": target_pod_info.get('name'),
            "pod_namespace": target_pod_info.get('namespace'),
            "cpu_exhaust": args.cpu_exhaust,
            "memory_exhaust": args.memory_exhaust,
            "spec_duration": duration
        }
    }

    if args.cpu_exhaust:
        start_event["parameters"]["cpu_intensity"] = cpu_intensity
    
    if args.memory_exhaust:
        start_event["parameters"]["memory_intensity"] = memory_intensity

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}. See producer log for error.")

    #Execute experiment
    try:
        logger.info(f"Starting resource exhaustion experiment on pod {target_pod_info['namespace']}/{target_pod_info['name']} (UID: {pod_uid})")
        
        #Run CPU stress if enabled
        if args.cpu_exhaust:
            logger.info(f"Running CPU stress with {cpu_intensity}% intensity")
            cpu_stress_test_success = cpu_stress_in_pod(api_client, target_pod_info, target_container_names, cpu_intensity, duration)
            logger.info(f"CPU stress test completed with success={cpu_stress_test_success}")
        
        #Run memory stress if enabled
        if args.memory_exhaust:
            logger.info(f"Running memory stress with {memory_intensity}% intensity")
            memory_stress_test_success = memory_stress_in_pod(api_client, target_pod_info, target_container_names, memory_intensity, duration)
            logger.info(f"Memory stress test completed with success={memory_stress_test_success}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while running load on CPU(s): {e}")
        if kafka_prod and kafka_prod.connected:
            error_event = {
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "source": "resource_exhaust",
                "parameters":
                    start_event["parameters"],
                "error": f"Process termination failed: {e}"
            }
            kafka_prod.send_event(error_event, experiment_id)

    #Ensure end event is always sent, kafka producer is always closed
    finally:
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        overall_success = True
        if args.cpu_exhaust and not cpu_stress_test_success:
            overall_success = False
        if args.memory_exhaust and not memory_stress_test_success:
            overall_success = False
            
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "source": "resource_exhaustion",
            "parameters":
                    start_event["parameters"],
            "success": overall_success,
            "duration": (end_time - start_time).total_seconds()
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event, experiment_id):
                logger.warning(f"Failed to send END event to Kafka for experiment {experiment_id}. See producer log for error.")

        kafka_prod.close()
        logger.info(f"Experiment {experiment_id} finished. Duration: {(end_time - start_time).total_seconds():.2f}s.")

if __name__ == "__main__":
    main()