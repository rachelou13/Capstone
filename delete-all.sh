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
    echo "Deleting resources in $dir"
    kubectl delete -f "$dir" --ignore-not-found
done
