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

def get_pod_controller(api_client, pod_name, namespace):
    try:
        core_v1 = client.CoreV1Api(api_client)
        pod = core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
        
        #Check for owner references
        if pod.metadata.owner_references:
            for ref in pod.metadata.owner_references:
                if ref.kind == "ReplicaSet":
                    #Find the deployment that owns this ReplicaSet
                    apps_v1 = client.AppsV1Api(api_client)
                    rs = apps_v1.read_namespaced_replica_set(name=ref.name, namespace=namespace)
                    if rs.metadata.owner_references:
                        for rs_ref in rs.metadata.owner_references:
                            if rs_ref.kind == "Deployment":
                                return "Deployment", rs_ref.name
                    return None, None
                elif ref.kind == "StatefulSet":
                    return "StatefulSet", ref.name
        
        return None, None
    except Exception as e:
        logger.error(f"Failed to determine pod controller: {e}")
        return None, None

def scale_controller(api_client, controller_type, controller_name, namespace, replicas):
    try:
        apps_v1 = client.AppsV1Api(api_client)
        logger.info(f"Scaling {controller_type} {namespace}/{controller_name} to {replicas} replicas")
        
        if controller_type == "Deployment":
            #Get current deployment
            controller = apps_v1.read_namespaced_deployment(name=controller_name, namespace=namespace)
            original_replicas = controller.spec.replicas
            
            #Scale to specified count
            controller.spec.replicas = replicas
            apps_v1.patch_namespaced_deployment(
                name=controller_name,
                namespace=namespace,
                body=controller
            )
        elif controller_type == "StatefulSet":
            #Get current statefulset
            controller = apps_v1.read_namespaced_stateful_set(name=controller_name, namespace=namespace)
            original_replicas = controller.spec.replicas
            
            #Scale to specified count
            controller.spec.replicas = replicas
            apps_v1.patch_namespaced_stateful_set(
                name=controller_name,
                namespace=namespace,
                body=controller
            )
        else:
            logger.error(f"Unsupported controller type: {controller_type}")
            return None
            
        return original_replicas
    except Exception as e:
        logger.error(f"Failed to scale {controller_type} {namespace}/{controller_name}: {e}")
        return None

def restart_controller(api_client, controller_type, controller_name, namespace):
    try:
        apps_v1 = client.AppsV1Api(api_client)
        logger.info(f"Restarting {controller_type} {namespace}/{controller_name}")
        
        if controller_type == "Deployment":
            #Get the deployment
            controller = apps_v1.read_namespaced_deployment(name=controller_name, namespace=namespace)
            
            #Add restart annotation
            if controller.spec.template.metadata.annotations is None:
                controller.spec.template.metadata.annotations = {}
            
            controller.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = datetime.now().isoformat()
            
            #Patch the deployment
            apps_v1.patch_namespaced_deployment(
                name=controller_name,
                namespace=namespace,
                body=controller
            )
        elif controller_type == "StatefulSet":
            #Get the statefulset
            controller = apps_v1.read_namespaced_stateful_set(name=controller_name, namespace=namespace)
            
            #Add restart annotation
            if controller.spec.template.metadata.annotations is None:
                controller.spec.template.metadata.annotations = {}
            
            controller.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = datetime.now().isoformat()
            
            #Patch the statefulset
            apps_v1.patch_namespaced_stateful_set(
                name=controller_name,
                namespace=namespace,
                body=controller
            )
        else:
            logger.error(f"Unsupported controller type: {controller_type}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Failed to restart {controller_type} {namespace}/{controller_name}: {e}")
        return False

def restart_deployment(api_client, deployment_name, namespace):
    """Legacy method kept for compatibility"""
    return restart_controller(api_client, "Deployment", deployment_name, namespace)

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
    logger.info("Running terminate pod experiment: V2")
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
    original_replicas = None
    controller_type = None
    controller_name = None

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

    #If deployment_name is provided, use it as Deployment type
    #Else, auto-detect the controller
    if deployment_name:
        controller_type = "Deployment"
        controller_name = deployment_name
        logger.info(f"Using provided deployment: {deployment_name}")
    else:
        #Auto-detect controller only if deployment_name is None
        controller_type, controller_name = get_pod_controller(api_client, pod_name, namespace)
        if controller_type and controller_name:
            logger.info(f"Auto-detected pod controller: {controller_type}/{controller_name}")
        else:
            logger.info("No deployment provided and no controller detected")

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
            "controller_type": controller_type,
            "controller_name": controller_name,
            "wait_duration": wait_duration
        }
    }
    
    if not kafka_prod.send_event(start_event, experiment_id):
        logger.warning(f"Failed to send START event to Kafka for experiment {experiment_id}")

    try:
        #If we have a controller, scale it to 0 first
        if controller_type and controller_name:
            original_replicas = scale_controller(api_client, controller_type, controller_name, namespace, 0)
            
            if original_replicas is None:
                raise Exception(f"Failed to scale {controller_type} to zero")
                
            #Give Kubernetes a moment to process the scaling
            time.sleep(5)
        
        apps_v1 = client.AppsV1Api(api_client)

        #Scale down to 0
        apps_v1.patch_namespaced_stateful_set(
            name="mysql-primary",
            namespace="default",
            body={"spec": {"replicas": 0}}
        )
        logger.info("💥 Scaled mysql-primary to 0")

        #Wait for specified duration
        logger.info(f"⏳ Waiting for {wait_duration} seconds before scaling up...")
        time.sleep(wait_duration)

        # Scale up to 1
        apps_v1.patch_namespaced_stateful_set(
            name="mysql-primary",
            namespace="default",
            body={"spec": {"replicas": 1}}
        )
        logger.info("🚀 Scaled mysql-primary back to 1")

        #Optional: restart annotation
        restart_successful = restart_controller(api_client, "StatefulSet", "mysql-primary", "default")

    except Exception as e:
        logger.error(f"Unexpected error during pod deletion experiment: {e}")
        #Ensure we scale back if there was an error
        if controller_type and controller_name and original_replicas is not None:
            try:
                scale_controller(api_client, controller_type, controller_name, namespace, original_replicas)
            except Exception as scale_error:
                logger.error(f"Error scaling back controller after failure: {scale_error}")
                
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
                "restart_successful": restart_successful,
                "controller_type": controller_type,
                "controller_name": controller_name,
                "original_replicas": original_replicas
            },
            "duration": duration
        }
        
        if not kafka_prod.send_event(end_event, experiment_id):
            logger.warning(f"Failed to send END event to Kafka for experiment {experiment_id}")
        
        kafka_prod.close()
        logger.info(f"Experiment {experiment_id} finished. Duration: {duration:.2f}s.")

if __name__ == "__main__":
    main()