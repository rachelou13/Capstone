import logging 
import os
import subprocess
import sys
import threading
import uuid
import time

from kubernetes import client, config
from python.data_scripts.infra_metrics_scraper import InfraMetricsScraper

title_separator = "="*6

#Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_k8s_client():
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except Exception as e:
        print(f"Error initializing Kubernetes client: {e}")
        print("Make sure you have the correct kubeconfig and kubernetes Python package installed.")
        return None

def find_pod_by_name(name_pattern, k8s_client):
    matching_pods = []
    
    try:
        pods = k8s_client.list_pod_for_all_namespaces(watch=False).items
        
        for pod in pods:
            pod_name = pod.metadata.name
            if name_pattern.lower() in pod_name.lower():
                matching_pods.append({
                    'name': pod_name,
                    'namespace': pod.metadata.namespace,
                    'uid': pod.metadata.uid,
                    'status': pod.status.phase,
                    'node': pod.spec.node_name
                })
        
        return matching_pods
    except Exception as e:
        print(f"Error finding pods: {e}")
        return []

def select_pod(k8s_client):
    while True:
        print("Parameters (press Enter to use default):")
        pod_name = input("Enter pod name (or part of name) to target (0 to cancel) [default: mysql-primary-0]: ").strip()
        
        if pod_name == "0":
            return None
            
        pod_name = pod_name if pod_name else "mysql-primary"
            
        matching_pods = find_pod_by_name(pod_name, k8s_client)
        
        if not matching_pods:
            print("No pods match that name. Please try again.")
            continue
            
        if len(matching_pods) == 1:
            selected_pod = matching_pods[0]
            print(f"\nSelected pod: {selected_pod['name']} in namespace {selected_pod['namespace']}")
            print(f"Status: {selected_pod['status']}")
            print(f"UID: {selected_pod['uid']}")
            print(f"Node: {selected_pod['node']}")
            
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
                print(f"Node: {selected_pod['node']}")
                
                confirm = input("\nConfirm selection? (y/n): ").strip().lower()
                if confirm == 'y':
                    return selected_pod
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def apply_resources():
    directories = [
        "k8s/configmaps",
        "k8s/secrets",
        "k8s/rbac",
        "k8s/services",
        "k8s/statefulsets",
        "k8s/deployments"
    ]
    
    for directory in directories:
        print(f"Applying resources in {directory}")
        try:
            result = subprocess.run(
                ["kubectl", "apply", "-f", directory], 
                capture_output=True, 
                text=True, 
                check=False
            )
            if result.returncode != 0:
                print(f"Failed to apply resources in {directory}")
                print(f"Error: {result.stderr}")
            else:
                print(f"Successfully applied resources in {directory}")
        except Exception as e:
            print(f"Error applying resources in {directory}: {e}")

def start_metrics_scraper(pod_info, scrape_interval=5):

    try:
        #Wait for Kafka to start up
        time.sleep(15)

        #Set unique id for scraper
        scraper_id = str(uuid.uuid4())

        #Initialize the scraper
        scraper = InfraMetricsScraper(
            scraper_id=scraper_id,
            target_pod_info=pod_info,
            scrape_interval=scrape_interval
        )

        #Check if /tmp exists and is writable
        if not os.path.exists('/tmp'):
            os.makedirs('/tmp', exist_ok=True)
            logger.debug("Created /tmp directory")
        
        if not os.access('/tmp', os.W_OK):
            logger.warning("/tmp directory is not writable")
        else:
            logger.debug("/tmp directory exists and is writable")
        
        #Save reference for other scripts to access
        InfraMetricsScraper.save_instance(scraper)
        
        #Start the scraper in a thread so it continues running after apply_all exits
        scraper_thread = threading.Thread(target=scraper.start, daemon=True)
        scraper_thread.start()
        
        print(f"Started infrastructure metrics scraper for pod {pod_info['namespace']}/{pod_info['name']}")
        print("Scraper will continue running in the background.")
        return scraper
    except Exception as e:
        print(f"Failed to start infrastructure metrics scraper: {e}")
        return None

def main():
    #K8s client setup
    k8s_client = get_k8s_client()
    if not k8s_client:
        print("Failed to initialize Kubernetes client. Exiting.")
        sys.exit(1)
    
    #Apply K8s resources
    print("\n" + title_separator + " APPLYING K8S CLUSTER " + title_separator + "\n")
    apply_resources()
    
    #Select a pod for monitoring
    print("\n" + title_separator + " SELECTING POD FOR MONITORING " + title_separator + "\n")
    pod_info = select_pod(k8s_client)
    if not pod_info:
        print("No pod selected. Exiting without starting the metrics scraper.")
        return
    
    #Get scrape interval
    scrape_interval = input("\nEnter metrics scrape interval in seconds [default: 5]: ").strip()
    try:
        scrape_interval = int(scrape_interval) if scrape_interval else 5
    except ValueError:
        print("Invalid input. Using default interval of 5 seconds.")
        scrape_interval = 5
    
    #Start the metrics scraper
    print("\n" + title_separator + " STARTING INFRASTRUCTURE METRICS SCRAPER " + title_separator + "\n")
    scraper = start_metrics_scraper(pod_info, scrape_interval)
    
    if scraper:
        print("\nInfrastructure metrics scraper is now running.")
        print("It will continue to run until stopped by delete_all.py.")
        print("\nYou can now use run_experiment.py to execute chaos experiments.")

if __name__ == "__main__":
    main()