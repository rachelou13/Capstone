#!/bin/bash

#MAKE SURE NO K8S PODS ARE RUNNING - RUN DELETE-ALL.SH IF NECESSARY

#Make sure kafka statefulset is running
kubectl apply -f k8s/statefulsets/kafka-statefulset.yaml

#Make sure kafka headless service is running
kubectl apply -f k8s/services/kafka-headless.yaml

#Scale statefulset to 0
kubectl scale statefulset kafka --replicas=0

#THIS WILL DELETE ALL KAFKA DATA
kubectl delete pvc kafka-data-kafka-0

#Scale statefulset back to 1
kubectl scale statefulset kafka --replicas=1

#Recreate topics
kubectl run -it kafka-client --image=bitnami/kafka:3.6.0 --rm --restart=Never -- bash

#Once inside kafka-client command line, run the below commands
<<COMMENT
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --list 

MAKE SURE YOU DON'T SEE ANY TOPICS - IF YOU DO DOUBLE CHECK THAT YOUR CONSUMER IS NOT RUNNNING AND RERUN SCRIPT

kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --create --topic chaos-events --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --create --topic infra-metrics --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --create --topic proxy-logs --partitions 1 --replication-factor 1

exit
COMMENT

sh apply-all.sh

#Wait for all containers to start
sleep 10

kubectl rollout restart deploy kafka-consumer