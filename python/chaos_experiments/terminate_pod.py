import logging
import argparse
import uuid
import time
from datetime import datetime, timezone

from kubernetes import client, config

from python.utils.kafka_producer import CapstoneKafkaProducer

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_pod(api_client, pod_name, namespace):
    try:
        core_v1 = client.CoreV1Api(api_client)
        logger.info(f"Deleting pod {namespace}/{pod_name}")
        
        #Force delete pod
        core_v1.delete_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            body=client.V1DeleteOptions(
                grace_period_seconds=0,
                propagation_policy="Background"
            )
        )
        return True
    except Exception as e:
        logger.error(f"Failed to delete pod {namespace}/{pod_name}: {e}")
        return False

def restart_deployment(api_client, deployment_name, namespace):
    try:
        apps_v1 = client.AppsV1Api(api_client)
        logger.info(f"Restarting deployment {namespace}/{deployment_name}")
        
        #Get the deployment
        deployment = apps_v1.read_namespaced_deployment(name=deployment_name, namespace=namespace)
        
        #Add restart annotation
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
        
        deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = datetime.now().isoformat()
        
        #Patch the deployment to trigger rollout
        apps_v1.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body=deployment
        )
        return True
    except Exception as e:
        logger.error(f"Failed to restart deployment {namespace}/{deployment_name}: {e}")
        return False

def wait_for_pod_recreation(api_client, pod_name, namespace, timeout=120):
    core_v1 = client.CoreV1Api(api_client)
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            #Check if pod exists and is running
            pod = core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
            if pod.status.phase == "Running":
                logger.info(f"Pod {namespace}/{pod_name} is now running")
                return True
        except client.exceptions.ApiException as e:
            if e.status != 404:
                logger.warning(f"Unexpected error checking pod status: {e}")
        
        time.sleep(5)
    
    logger.error(f"Timed out waiting for pod {namespace}/{pod_name} to be recreated")
    return False

def main():
    parser = argparse.ArgumentParser(description="Delete and restart a pod after specified duration")
    parser.add_argument(
        "-p",
        "--pod-name",
        type=str,
        required=True,
        help="Name of the pod to delete"
    )
    parser.add_argument(
        "-n",
        "--namespace",
        type=str,
        default="default",
        help="Namespace of the pod"
    )
    parser.add_argument(
        "-d",
        "--deployment-name",
        type=str,
        required=False,
        default=None,
        help="Name of the deployment if pod is part of one"
    )
    parser.add_argument(
        "-w",
        "--wait-duration",
        type=int,
        required=False,
        default=30,
        help="Duration to wait before restarting (seconds)"
    )
    parser.add_argument(
        "-k",
        "--kube-config",
        type=str,
        required=False,
        default="~/.kube/config",
        help="Path to kubeconfig file"
    )
    
    args = parser.parse_args()
    pod_name = args.pod_name
    namespace = args.namespace
    deployment_name = args.deployment_name
    wait_duration = args.wait_duration
    kube_config = args.kube_config

    #Start kafka producer
    kafka_prod = CapstoneKafkaProducer()
    experiment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    #Initialize success trackers
    deletion_successful = False
    restart_successful = False

    #K8s client setup
    try:
        if kube_config:
            config.load_kube_config(config_file=kube_config)
        else:
            config.load_incluster_config()
        api_client = client.ApiClient()
        logger.info("Kubernetes client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Kubernetes client: {e}")
        if kafka_prod and kafka_prod.connected:
            kafka_prod.send_event({
                "timestamp": start_time.isoformat(), 
                "experiment_id": experiment_id, 
                "event_type": "error",
                "source": "terminate_pod", 
                "error": f"K8s client init failed: {e}"
            }, experiment_id)
        return

    #Send start event to kafka
    start_event = {
        "timestamp": start_time.isoformat(),
        "experiment_id": experiment_id,
        "event_type": "start",
        "source": "terminate_pod",
        "parameters": {
            "pod_name": pod_name,
            "namespace": namespace,
            "deployment_name": deployment_name,
            "wait_duration": wait_duration
        }
    }
    
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}")

    try:
        #Delete the pod
        logger.info(f"Deleting pod {namespace}/{pod_name}")
        deletion_successful = delete_pod(api_client, pod_name, namespace)
        
        if deletion_successful:
            #Wait for specified duration
            logger.info(f"Waiting for {wait_duration} seconds before restart")
            time.sleep(wait_duration)
            
            #Restart the pod or deployment
            if deployment_name:
                restart_successful = restart_deployment(api_client, deployment_name, namespace)
            else:
                #For statefulsets or pods not managed by deployments
                logger.info("No deployment specified, waiting for pod to be automatically recreated")
                restart_successful = wait_for_pod_recreation(api_client, pod_name, namespace)
    except Exception as e:
        logger.error(f"Unexpected error during pod deletion experiment: {e}")
        if kafka_prod and kafka_prod.connected:
            kafka_prod.send_event({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "experiment_id": experiment_id,
                "event_type": "error",
                "source": "terminate_pod",
                "parameters": start_event["parameters"],
                "error": f"Experiment failed: {e}"
            }, experiment_id)
    
    finally:
        #Send end event to kafka
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        end_event = {
            "timestamp": end_time.isoformat(),
            "experiment_id": experiment_id,
            "event_type": "end",
            "source": "terminate_pod",
            "parameters": start_event["parameters"],
            "success": deletion_successful and restart_successful,
            "details": {
                "deletion_successful": deletion_successful,
                "restart_successful": restart_successful
            },
            "duration": duration
        }
        
        if not kafka_prod.send_event(end_event, experiment_id):
            logger.warning(f"Failed to send END event to Kafka for experiment {experiment_id}")
        
        kafka_prod.close()
        logger.info(f"Experiment {experiment_id} finished. Duration: {duration:.2f}s.")

if __name__ == "__main__":
    main()