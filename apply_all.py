import logging 
import subprocess
import sys

from kubernetes import client, config
from python.data_scripts.metrics_scraper import KafkaMetricsProducer

title_separator = "="*6

#Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_k8s_client():
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except Exception as e:
        print(f"⚠️ Error initializing Kubernetes client: {e}")
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
        print(f"⚠️ Error finding pods: {e}")
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
            print("❌ No pods match that name. Please try again.")
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
        print("\n❔ Multiple pods match your input. Please select one:")
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
                print("❌ Invalid selection. Please try again.")
        except ValueError:
            print("❌ Please enter a valid number.")

def create_metrics_scraper_config(pod_info, scrape_interval=5):
    try:
        print("⏳ Creating metrics scraper configuration...")
        
        #Create a ConfigMap for storing pod info - add a proper name field
        configmap_yaml = f"""
            apiVersion: v1
            kind: ConfigMap
            metadata:
                name: metrics-scraper-config
            data:
                TARGET_POD_NAME: "{pod_info['name']}"
                TARGET_POD_NAMESPACE: "{pod_info['namespace']}"
                TARGET_POD_UID: "{pod_info['uid']}"
                SCRAPE_INTERVAL: "{scrape_interval}"
        """
        # Try writing to /tmp first, fall back to current directory if needed
        config_file_path = '/tmp/metrics-scraper-config.yaml'
        try:
            # Test if we can write to the directory
            with open(config_file_path, 'w') as test_file:
                test_file.write('test')
        except PermissionError:
            # Fall back to current directory
            config_file_path = './metrics-scraper-config.yaml'
            print(f"⚠️ Cannot write to /tmp, using current directory instead: {config_file_path}")

        #Save ConfigMap YAML to a temporary file
        with open(config_file_path, 'w') as f:
            f.write(configmap_yaml)
        
        #Apply the ConfigMap
        try:
            result = subprocess.run(
                ["kubectl", "apply", "-f", config_file_path],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode != 0:
                print(f"❌ Failed to create metrics scraper ConfigMap")
                print(f"⚠️ Error: {result.stderr}")
                return False
            else:
                print(f"✅ Successfully created metrics scraper ConfigMap")
                return True
        except Exception as e:
            print(f"⚠️ Error creating metrics scraper ConfigMap: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to set up metrics scraper configuration: {e}")
        return False

def update_metrics_scraper_deployment(pod_info):
    try:
        # Update the deployment to target the selected pod
        result = subprocess.run(
            ["kubectl", "patch", "deployment", "metrics-scraper", "-p", 
             f'{{"spec":{{"template":{{"spec":{{"containers":[{{"name":"scraper","env":[{{"name":"TARGET_POD_NAME","value":"{pod_info["name"]}"}},{{"name":"TARGET_POD_NAMESPACE","value":"{pod_info["namespace"]}"}}]}}]}}}}}}}}'],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            print(f"❌ Failed to update metrics scraper deployment")
            print(f"⚠️ Error: {result.stderr}")
            return False
        else:
            print(f"✅ Successfully updated metrics scraper deployment")
            # Restart the deployment to apply changes
            subprocess.run(["kubectl", "rollout", "restart", "deployment", "metrics-scraper"])
            return True
    except Exception as e:
        print(f"⚠️ Error updating metrics scraper deployment: {e}")
        return False

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
                print(f"❌ Failed to apply resources in {directory}")
                print(f"⚠️ Error: {result.stderr}")
            else:
                print(f"✅ Successfully applied resources in {directory}")
        except Exception as e:
            print(f"⚠️ Error applying resources in {directory}: {e}")

def main():
    #K8s client setup
    k8s_client = get_k8s_client()
    if not k8s_client:
        print("❌ Failed to initialize Kubernetes client. Exiting.")
        sys.exit(1)
    
    #Apply K8s resources
    print("\n" + title_separator + " APPLYING K8S CLUSTER " + title_separator + "\n")
    apply_resources()
    
    #Select a pod for monitoring
    print("\n" + title_separator + " SELECTING POD FOR MONITORING " + title_separator + "\n")
    pod_info = select_pod(k8s_client)
    if not pod_info:
        print("❌ No pod selected. Exiting without starting the metrics scraper.")
        return
    
    #Get scrape interval
    scrape_interval = input("\nEnter metrics scrape interval in seconds [default: 5]: ").strip()
    try:
        scrape_interval = int(scrape_interval) if scrape_interval else 5
    except ValueError:
        print("Invalid input. Using default interval of 5 seconds.")
        scrape_interval = 5
    
    #Configure metrics scraper
    print("\n" + title_separator + " CONFIGURING INFRASTRUCTURE METRICS SCRAPER " + title_separator + "\n")
    create_metrics_scraper_config(pod_info, scrape_interval)
    update_metrics_scraper_deployment(pod_info)
    
    print("\n✅ Infrastructure metrics scraper has been configured and deployed.")
    print("\n💣 You can now use run_experiment.py to execute chaos experiments.")

if __name__ == "__main__":
    main()