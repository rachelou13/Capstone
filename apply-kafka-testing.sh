#!/bin/bash

# List of sources
sources=(
    "k8s/deployments/test-app-deployment.yaml"
    "k8s/services/kafka-headless.yaml"
    "k8s/services/kafka-nodeport.yaml"
    "k8s/statefulsets"
)

for source in "${sources[@]}"; do
    echo "Applying resources in $source"
    kubectl apply -f "$source"
done
