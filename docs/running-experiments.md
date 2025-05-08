## Get Pod UID
```sh
kubectl get pod <pod-name> -o jsonpath='{.metadata.uid}'
```

## Run Terminate Process Experiment
```sh
python -m python.chaos_experiments.terminate_process -u <UID of target pod>
```
