# Using CLI app

In the root Capstone folder, run
```sh
python run_experiment.py
```

Follow the prompts and execute the experiments you would like. Entering 0 at any point will return you to the main menu.

#Using the command line

## Get Pod UID
```sh
kubectl get pod <pod-name> -o jsonpath='{.metadata.uid}'
```

## Run Terminate Process Experiment
```sh
python -m python.chaos_experiments.terminate_process -u <UID of target pod>
```
