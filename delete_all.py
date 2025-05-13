import os
import subprocess
import sys
import time
import logging

from python.data_scripts.infra_metrics_scraper import InfraMetricsScraper

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
                print(f"Failed to delete resources in {directory}")
                print(f"Error: {result.stderr}")
            else:
                print(f"Successfully deleted resources in {directory}")
        except Exception as e:
            print(f"Error deleting resources in {directory}: {e}")

def stop_metrics_scraper():
    try:
        #Get the running scraper instance
        scraper = InfraMetricsScraper.get_instance()
        
        if scraper:
            print("Found running infrastructure metrics scraper. Closing...")
            
            #Close the scraper
            scraper.close()
            print("Scraper shutdown completed")
            
            #Remove the pickle file
            try:
                os.remove('/tmp/infra_scraper.pickle')
                logger.info("Removed scraper instance file")
            except FileNotFoundError:
                logger.warning("Scraper instance file not found")
            except Exception as e:
                logger.error(f"Error removing scraper instance file: {e}")
                
            return True
        else:
            logger.warning("No running infrastructure metrics scraper found")
            return False
            
    except Exception as e:
        logger.error(f"Error stopping infrastructure metrics scraper: {e}")
        return False

def main():
    #Stop the metrics scraper
    print("\n" + title_separator + " STOPPING INFRASTRUCTURE METRICS SCRAPER " + title_separator + "\n")
    scraper_stopped = stop_metrics_scraper()
    
    #Allow time for scraper to shut down
    if scraper_stopped:
        print("Waiting for scraper to finish...")
        time.sleep(5)
    
    #Delete Kubernetes resources
    print("\n" + title_separator + " DELETING K8S CLUSTER " + title_separator + "\n")
    delete_resources()
    
    print("Cleanup completed")

if __name__ == "__main__":
    main()