import subprocess
import time
import logging

title_separator = "="*6

#Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_resources():
    directories = [
        "k8s/deployments",
        "k8s/statefulsets", 
        "k8s/services",
        "k8s/rbac",
        "k8s/secrets",
        "k8s/configmaps"
    ]
    
    for directory in directories:
        print(f"Deleting resources in {directory}")
        try:
            result = subprocess.run(
                ["kubectl", "delete", "-f", directory], 
                capture_output=True, 
                text=True, 
                check=False
            )
            if result.returncode != 0:
                print(f"❌ Failed to delete resources in {directory}")
                print(f"⚠️ Error: {result.stderr}")
            else:
                print(f"✅ Successfully deleted resources in {directory}")
        except Exception as e:
            print(f"⚠️ Error deleting resources in {directory}: {e}")

def stop_metrics_scraper():
    try:
        print("⏳ Stopping metrics-scraper deployment...")
        #Scale down the deployment to 0 replicas
        result = subprocess.run(
            ["kubectl", "scale", "deployment", "metrics-scraper", "--replicas=0"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            print(f"❌ Failed to stop metrics-scraper deployment")
            print(f"⚠️ Error: {result.stderr}")
            return False
        else:
            print(f"✅ Successfully stopped metrics-scraper deployment")
            return True
    except Exception as e:
        print(f"⚠️ Error stopping metrics-scraper deployment: {e}")
        return False

def main():
    #Stop the metrics scraper
    print("\n" + title_separator + " STOPPING INFRASTRUCTURE METRICS SCRAPER " + title_separator + "\n")
    scraper_stopped = stop_metrics_scraper()
    
    #Allow time for scraper to shut down
    if scraper_stopped:
        print("⏳ Waiting for scraper to finish...")
        time.sleep(5)
    
    #Delete Kubernetes resources
    print("\n" + title_separator + " DELETING K8S CLUSTER " + title_separator + "\n")
    delete_resources()
    
    print("\n✅ Cleanup completed")

if __name__ == "__main__":
    main()