import logging
import psutil
import argparse
from chaos_engineering_scripts.utils.kafka_producer import ChaosKafkaProducer
import uuid
from datetime import datetime, timezone
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_cgroup_info(pid):
    logger.info(f"Attempting to retrieve cgroup information for PID {pid}")
    try:
        with open(f"proc/{pid}/cgroup", "r") as f:
            return f.read()
    except FileNotFoundError:
        logger.warning(f"Cgroup file for PID {pid} not found - likely exited")
        return ""
    except PermissionError:
        logger.warning(f"Permission denied reading cgroup file for PID {pid} - skipping")
        return ""
    except Exception as e:
        logger.warning(f"Unexpected error occurred reading cgroup file for PID {pid} - skipping")
        return ""

def find_and_terminate_process(pod_uid, container_id_prefix, process_pattern):
    process_terminated = False
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Don't KYS
            if p.pid == os.getpid():
                continue
            
            #Get cgroup info
            cgroup_content = get_cgroup_info(p.pid)
           
            #Initialize variables for matching
            pod_uid_found = False
            target_cgroup_line = None
           
            #Search for pod UID in cgroup file
            for line in cgroup_content.splitlines():
                if "kubepods" in line and pod_uid in line:
                    pod_uid_found = True
                    target_cgroup_line = line
                    break

            if not pod_uid_found:
                continue

            logger.info(f"PID {p.pid} ({p.info.get('name', 'N/A')}) cgroup information suggests it's in pod {pod_uid}")

            #Search for container prefix IF PROVIDED
            if container_id_prefix:
                if not target_cgroup_line or container_id_prefix not in target_cgroup_line:
                    logger.info(f"PID {p.pid} is in pod {pod_uid}, but not in container with prefix {container_id_prefix} - based on cgroup line {target_cgroup_line}")
                    continue
            
            logger.info(f"PID {p.pid} ({p.info.get('name', 'N/A')}) cgroup information suggests it's in container with prefix {container_id_prefix} of pod {pod_uid}")

            #Search for process patterin IF PROVIDED - assume it is not by default
            process_matches_pattern = True
            if process_pattern:
                process_matches_pattern = False
                process_cmdline = ""
                if p.info['cmdline']:
                    process_cmdline = " ".join(p.info['cmdline'])
                if (process_pattern in process_cmdline or (p.info['name'] and process_pattern in p.info['name'])):
                    process_matches_pattern = True
                else:
                    logger.warning(f"PID {p.pid} ({p.info.get('name', 'N/A')}) in target pod/container does not match process pattern provided: {process_pattern} - based on process cmdline {process_cmdline}")
                
            if not process_matches_pattern:
                continue

            #ADD LOGIC TO TERMINATE HERE

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
        except Exception as e:
            logger.error(f"Could not inspect process {p.pid if p else 'N/A'}: {e}")
    return process_terminated
        

def main():
    #Parse args from command line
    parser = argparse.ArgumentParser(description="Find and terminate processes based on command-line pattern")
    parser.add_argument(
        "-u",
        "--pod-uid",
        required=True,
        type=str,
        metavar="POD_UID",
        help="UID of the target Kubernetes pod"
    )
    parser.add_argument(
        "-c",
        "--container-id-prefix",
        required=False,
        type=str,
        default=None,
        metavar="CONTAINER_ID_PREFIX",
        help="Prefix of the target container ID. If the pod only has one container or you wish to target all containers in the pod matching the process pattern, omit this"
    )
    parser.add_argument(
        "-p",
        "--process-pattern",
        required=False,
        type=str,
        default=None,
        metavar="PROCESS_PATTERN",
        help="String pattern to search for within the command line or name of processes running inside target pod"
    )
    args = parser.parse_args()
    pod_uid = args.pod_uid
    container_id_prefix = args.container_id_prefix
    process_pattern = args.process_pattern

    #Start kafka producer
    kafka_prod = ChaosKafkaProducer()
    experiment_id = str(uuid.uuid4())

    #Send start event to kafka
    start_time = datetime.now(timezone.utc)
    start_event = {
        "timestamp": start_time.isoformat(),
        "experiment_id": experiment_id,
        "event_type": "start",
        "experiment_type": "terminate_process",
        "parameters": {
            "pod_uid": pod_uid,
            "container_id_prefix": container_id_prefix,
            "process_pattern": process_pattern
        }
    }

    #Kafka producer send event function returns True if successful, False if failed
    if not kafka_prod.send_event(start_event):
        logger.warning(f"Failed to send start event to Kafka. See log for error.")

    try:
        logger.info(f"Targeting parameters: {pod_uid}" +  (f"- {container_id_prefix}" if {container_id_prefix} else "") + (f"- {process_pattern}" if {process_pattern} else ""))
        process_terminated = find_and_terminate_process(pod_uid, container_id_prefix, process_pattern)
    except Exception as e:
        logger.error(f"An unexpected error occurred while terminating process(es): {e}")
    #Ensure end event is always sent, kafka producer is always closed
    finally:
        print('BONK')
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "experiment_type": "terminate_process",
            "parameters": {
                "pod_uid": pod_uid,
                "container_id_prefix": container_id_prefix,
                "process_pattern": process_pattern
            },
            "success": process_terminated,
            "duration": (end_time - start_time).total_seconds()
        }

        #Kafka producer send event function returns True if successful, False if failed
        if not kafka_prod.send_event(end_event):
                logger.warning(f"Failed to end start event to Kafka. See log for error.")

        kafka_prod.close()

        if not process_terminated:
            logger.warning(f"No running processes found matching parameters: {pod_uid}" +  (f"- {container_id_prefix}" if {container_id_prefix} else "") + (f"- {process_pattern}" if {process_pattern} else "") + "(or failed to terminate processes)")
        else:
            logger.info(f"Finished finding and terminating processes matching parameters: {pod_uid}" +  (f"- {container_id_prefix}" if {container_id_prefix} else "") + (f"- {process_pattern}" if {process_pattern} else ""))

if __name__ == "__main__":
    main()