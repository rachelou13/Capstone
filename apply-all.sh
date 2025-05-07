#!/bin/bash

# List of directories
dirs=(
    "k8s/configmaps"
    "k8s/deployments"
    "k8s/rbac"
    "k8s/secrets"
    "k8s/services"
    "k8s/statefulsets"
)

for dir in "${dirs[@]}"; do
    echo "Applying resources in $dir"
    kubectl apply -f "$dir"
done
