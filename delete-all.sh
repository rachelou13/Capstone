#!/bin/bash

# List of directories
dirs=(
  "k8s/configmaps"
  "k8s/secrets"
  "k8s/rbac"
  "k8s/services"
  "k8s/statefulsets"
  "k8s/deployments"
)


for dir in "${dirs[@]}"; do
    echo "Applying resources in $dir"
    kubectl delete -f "$dir" || echo "Failed to delete resources in $dir"
done
