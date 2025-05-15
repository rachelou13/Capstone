import logging
import subprocess
import sys
from kubernetes import client, config

title_separator = "="*6

#Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def display_menu():
    print("\n" + title_separator + " CHAOS EXPERIMENT LAUNCHER " + title_separator + "\n")
    print("Available experiments: ")
    print("1. Process Termination")
    print("2. Pod Termination")
    print("3. Resource Exhaustion (CPU stress test)")
    print("4. Network Partition")
    print("0. Exit")

def get_k8s_client():
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except Exception as e:
        print(f"Error initializing Kubernetes client: {e}")
        print("Make sure you have the correct kubeconfig and kubernetes Python package installed.")
        return None

def find_pod_by_name(k8s_client, name_pattern):
    matching_pods = []
    apps_v1 = client.AppsV1Api(k8s_client.api_client)
    
    try:
        pods = k8s_client.list_pod_for_all_namespaces(watch=False).items
        
        for pod in pods:
            pod_name = pod.metadata.name
            if name_pattern.lower() in pod_name.lower():
                #Extract deployment information from pod labels
                deployment_name = None
                if pod.metadata.labels and 'app' in pod.metadata.labels:
                    app_label = pod.metadata.labels['app']
                    #Try to find deployments matching this label
                    try:
                        deployments = apps_v1.list_namespaced_deployment(
                            namespace=pod.metadata.namespace,
                            label_selector=f"app={app_label}"
                        ).items
                        if deployments:
                            deployment_name = deployments[0].metadata.name
                    except Exception as e:
                        logging.debug(f"Error finding deployment: {e}")
                
                #If that fails try using owner references to find deployment
                if not deployment_name and pod.metadata.owner_references:
                    for owner in pod.metadata.owner_references:
                        if owner.kind == 'ReplicaSet':
                            try:
                                rs = apps_v1.read_namespaced_replica_set(
                                    name=owner.name,
                                    namespace=pod.metadata.namespace
                                )
                                if rs.metadata.owner_references:
                                    for rs_owner in rs.metadata.owner_references:
                                        if rs_owner.kind == 'Deployment':
                                            deployment_name = rs_owner.name
                                            break
                            except Exception as e:
                                logging.debug(f"Error finding replicaset owner: {e}")
                
                matching_pods.append({
                    'name': pod_name,
                    'deployment': deployment_name,
                    'namespace': pod.metadata.namespace,
                    'uid': pod.metadata.uid,
                    'status': pod.status.phase
                })
        
        return matching_pods
    except Exception as e:
        print(f"Error finding pods: {e}")
        return []

def select_pod(k8s_client, pod_name=""):
    while True:
        if not pod_name.strip():
            pod_name = input("\nEnter pod name (or part of name) to target (0 to return to main menu): ").strip()
        else:
            print(f"\nTargeting the below pod for network partition:")
        
        if pod_name == "0":
            return None
            
        if not pod_name:
            print("Pod name cannot be empty. Please try again.")
            continue
            
        matching_pods = find_pod_by_name(k8s_client, pod_name)
        
        if not matching_pods:
            print("No pods match that name. Please try again.")
            continue
            
        if len(matching_pods) == 1:
            selected_pod = matching_pods[0]
            print(f"\nPod: {selected_pod['name']} in namespace {selected_pod['namespace']}")
            print(f"Status: {selected_pod['status']}")
            print(f"UID: {selected_pod['uid']}")
            
            confirm = input("\nConfirm selection? (y/n): ").strip().lower()
            if confirm == 'y':
                return selected_pod
            continue
            
        #Multiple matches - display list for selection
        print("\nMultiple pods match your input. Please select one:")
        for i, pod in enumerate(matching_pods, 1):
            print(f"{i}. {pod['namespace']}/{pod['name']} ({pod['status']})")
            
        selection = input("\nEnter number to select (0 to try again): ").strip()
        
        if selection == "0":
            continue
            
        try:
            index = int(selection) - 1
            if 0 <= index < len(matching_pods):
                selected_pod = matching_pods[index]
                print(f"\nSelected pod: {selected_pod['name']} in namespace {selected_pod['namespace']}")
                print(f"Status: {selected_pod['status']}")
                print(f"UID: {selected_pod['uid']}")
                
                confirm = input("\nConfirm selection? (y/n): ").strip().lower()
                if confirm == 'y':
                    return selected_pod
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def run_network_partition(pod_info):
    print("\n" + title_separator + " NETWORK PARTITION EXPERIMENT " + title_separator  + "\n")
    print("Parameters (press Enter to use default):")
    
    #Get target host
    target_service = input("Target service to block [default: mysql-primary]: ").strip()
    target_service = target_service if target_service else "mysql-primary"
    
    #Get port
    port = input("Port to block [default: 3306]: ").strip()
    port = port if port else "3306"
    
    #Get protocol
    protocol = input("Protocol (tcp/udp/icmp) [default: tcp]: ").strip().lower()
    protocol = protocol if protocol in ["tcp", "udp", "icmp"] else "tcp"
    
    #Get duration
    duration = input("Duration in seconds [default: 30]: ").strip()
    duration = duration if duration else "30"
    
    print("\nRunning Network Partition experiment with the following parameters:")
    print(f"Pod: {pod_info['namespace']}/{pod_info['name']}")
    print(f"UID: {pod_info['uid']}")
    print(f"Target Service: {target_service}")
    print(f"Port: {port}")
    print(f"Protocol: {protocol}")
    print(f"Duration: {duration} seconds")
    
    confirm = input("\nExecute experiment? (y/n): ").strip().lower()
    if confirm != 'y':
        return
    
    cmd = [
        sys.executable, "-m", "python.chaos_experiments.network_partition",
        "-u", pod_info['uid'],
        "-ts", target_service,
        "-p", port,
        "-pr", protocol,
        "-d", duration
    ]
    
    try:
        print("\nExecuting experiment...")
        subprocess.run(cmd, check=True)
        print("\nExperiment completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\nError running experiment: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    
    input("\nPress Enter to continue...")

def run_resource_exhaustion(pod_info):
    print("\n" + title_separator + " RESOURCE EXHAUSTION EXPERIMENT " + title_separator  + "\n")
    print("Parameters (press Enter to use default):")
    
    #Get CPU cores
    cores = input("Number of CPU cores to stress [default: 2]: ").strip()
    cores = cores if cores else "2"
    
    #Get duration
    duration = input("Duration in seconds [default: 30]: ").strip()
    duration = duration if duration else "30"
    
    print("\nRunning Resource Exhaustion experiment with the following parameters:")
    print(f"Pod: {pod_info['namespace']}/{pod_info['name']}")
    print(f"UID: {pod_info['uid']}")
    print(f"CPU Cores: {cores}")
    print(f"Duration: {duration} seconds")
    
    confirm = input("\nExecute experiment? (y/n): ").strip().lower()
    if confirm != 'y':
        return
    
    cmd = [
        sys.executable, "-m", "python.chaos_experiments.resource_exhaust",
        "-u", pod_info['uid'],
        "-c", cores,
        "-d", duration
    ]
    
    try:
        print("\nExecuting experiment...")
        subprocess.run(cmd, check=True)
        print("\nExperiment completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\nError running experiment: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    
    input("\nPress Enter to continue...")

def run_pod_termination(pod_info):
    print("\n" + title_separator + " POD TERMINATION EXPERIMENT " + title_separator  + "\n")
    print("Parameters (press Enter to use default):")

    #Get wait duration
    wait_duration = input("Duration in seconds to wait before restarting pod [default: 30]: ")
    wait_duration = wait_duration if wait_duration else "30"

    print("\nRunning Pod Termination experiment with the following parameters:")
    print(f"Pod: {pod_info['namespace']}/{pod_info['name']}")
    if pod_info['deployment'] is not None:
        print(f"Deployment: {pod_info['deployment']}")
    print(f"Duration: {wait_duration} seconds")

    confirm = input("\nExecute experiment? (y/n): ").strip().lower()
    if confirm != 'y':
        return
    
    cmd = [
        sys.executable, "-m", "python.chaos_experiments.terminate_pod",
        "-p", pod_info['name'],
        "-n", pod_info['namespace'],
        "-w", wait_duration
    ]

    if pod_info['deployment']:
        cmd.extend(["-d", pod_info['deployment']])

    try:
        print("\nExecuting experiment...")
        subprocess.run(cmd, check=True)
        print("\nExperiment completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\nError running experiment: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    
    input("\nPress Enter to continue...")

def run_process_termination(pod_info):
    print("\n" + title_separator + " PROCESS TERMINATION EXPERIMENT " + title_separator  + "\n")
    print("Parameters (press Enter to skip):")
    
    #Get container ID prefix (optional)
    container_id = input("Container ID prefix (optional): ").strip()
    
    #Get process pattern (optional)
    process_pattern = input("Process pattern to match (optional): ").strip()
    
    print("\nRunning Process Termination experiment with the following parameters:")
    print(f"Pod: {pod_info['namespace']}/{pod_info['name']}")
    print(f"UID: {pod_info['uid']}")
    if container_id:
        print(f"Container ID Prefix: {container_id}")
    if process_pattern:
        print(f"Process Pattern: {process_pattern}")
    
    confirm = input("\nExecute experiment? (y/n): ").strip().lower()
    if confirm != 'y':
        return
    
    cmd = [sys.executable, "-m", "python.chaos_experiments.terminate_process", "-u", pod_info['uid']]
    
    if container_id:
        cmd.extend(["-c", container_id])
    
    if process_pattern:
        cmd.extend(["-p", process_pattern])
    
    try:
        print("\nExecuting experiment...")
        subprocess.run(cmd, check=True)
        print("\nExperiment completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\nError running experiment: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    
    input("\nPress Enter to continue...")

def main():
    #K8s client setup
    k8s_client = get_k8s_client()
    if not k8s_client:
        print("Failed to initialize Kubernetes client. Exiting.")
        sys.exit(1)
    
    while True:
        display_menu()
        
        choice = input("Select an option: ").strip()

        pod_name = "python-proxy"
        
        if choice == "0":
            print("\nExiting. Goodbye!")
            break
        
        if choice not in ["1", "2", "3"]:
            print("\nInvalid choice. Please try again.")
            input("Press Enter to continue...")
            continue
                    
        #Run experiment
        if choice == "4":
            pod_info = select_pod(k8s_client, pod_name=pod_name)
            run_network_partition(pod_info)
        else:
            #Select target pod
            pod_info = select_pod(k8s_client)
            if not pod_info:
                continue
            if choice == "1":
                run_process_termination(pod_info)
            elif choice == "2":
                run_pod_termination(pod_info)
            elif choice == "3":
                run_resource_exhaustion(pod_info)
            

if __name__ == "__main__":
    main()