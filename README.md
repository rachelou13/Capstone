# PNC Early Career SRE Bootcamp Capstone Project

![Docker](https://img.shields.io/badge/Containerization-Docker-2496ED?logo=docker) 
![Kubernetes](https://img.shields.io/badge/Orchestration-Kubernetes-326CE5?logo=kubernetes)
![Python](https://img.shields.io/badge/Scripting-Python_3.11-3776AB?logo=python&logoColor=ffe263)
![MySQL](https://img.shields.io/badge/Database-MySQL-4479A1?logo=mysql&logoColor=white)
![MongoDB](https://img.shields.io/badge/Database-MongoDB-47A248?logo=mongodb)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-white?logo=apachekafka)
![Monitoring](https://img.shields.io/badge/Monitoring-Prometheus-E6522C?logo=prometheus)
![Monitoring](https://img.shields.io/badge/Visualization-Grafana-F46800?logo=grafana)

## Project Summary

Capstone Project of the February 2025 cohort of the PNC/TEKsystems Early Career SRE bootcamp. This is a disaster recovery testing platform that offers a CLI to run multiple types of failure simulations built-in recovery automation and system health monitoring/visualization. 

## Quick Links
[Prerequisite Installation](#prequisite-installation) \
[Environment Set-Up](#environment-set-up) \
[Usage](#usage) \
[Credits](#credits)

# Prequisite Installation

Before beginning, you must have the following installed:
* Docker Desktop with Kubernetes
* Python
* Pip (Package Manager)

If you do not, follow the instructions below
## Docker Desktop
1. Navigate to [Docs.Docker.com](https://www.docker.com/products/docker-desktop/)
2. Download the correct installation for your machine
3. Run the executable installer
4. Follow the set-up instructions. The recommended defaults will work for this project. 
5. Restart your machine if necessary

## Kubernetes
1. Start up Docker
2. Click on the Settings icon in the top toolbar
3. Click on 'Kubernetes' in the left sidebar. 
4. Turn on the 'Enable Kubernetes' option 
5. Click 'Apply & Restart' 
6. Click 'Install'

Once installation has finished, you should see 'Kubernetes running' along the bottom of the application window.

## Python
1. Navigate to [Python.org](https://www.python.org/downloads/)
2. Download the correct installation for your machine
3. Run the executable installer
4. Follow the set-up instructions. The recommended defaults will work for this project.
5. Restart your machine if necessary

## Pip
Once Python is installed, run the following command to ensure Pip is installed:
```sh
python -m pip install --upgrade pip
```

# Environment Set-Up

## Installing Required Python Packages
Skip this section and go to the next section if you would prefer to set up a virtual environment. 

1. Navigate to the main project folder on your local machine
2. Run ```ls``` to ensure that you are in the folder containing requirements.txt
3. Run ```pip install -r requirements.txt```

## Setting Python Virtual Environment (Optional)
1. Navigate to the main project folder on your local machine
2. Run ```ls``` to ensure that you are in the folder containing requirements.txt
3. Run ```python -m venv .venv```
4. Run ```source .venv/Scripts/activate```
5. You are now in your virtual environment and should see (.venv) at the start of each line in your terminal
6. Run ```python -m pip install -r requirements.txt``` \
    a. You may use ```pip install -r requirements.txt``` however, using ```python -m``` as a preface ensures you are using the ```pip``` associated with the currently active Python interpreter (the one inside the virtual environment). This is a good practice. 
7. Check the correct packages are installed by running ```python -m pip freeze```
### Setting Your Virtual Environment in VS Code
1. Open your main project folder in VS Code
2. Open the Command Palette with the shortcut Ctrl+Shift+P
3. Search for and select 'Python: Select Interpreter'
4. Select 'Enter interpreter path...'
5. Select 'Find...'
6. Browse to '.venv\Scripts' within your capstone folder and select 'python.exe'
### Exiting the Virtual Environment
1. When you are done working in the virtual environment, simply run ```deactivate``` in your terminal

## Deploying the Cluster
Once all prerequisites are installed and Docker Desktop with Kubernetes is running, navigate to the main project folder and run the apply_all.py file using the following command:
```sh
python apply_all.py
```
1. Press Enter to monitor the default pod with the infrastructure metrics scraper
2. Enter 'y' to confirm
3. Press Enter to use the default scrape interval of 5 seconds, or enter a different integer value

## Viewing and Managing Kafka Topics
Kafka in this cluster is configured to automatically create the required topics. However, the below commands can be used to view and manage the topics as well as test the broker in Kafka if needed.

```sh
# Confirm the Kafka broker is running
kubectl get pods -l app=kafka

# Launch Kafka Client Pod
# You should get a shell prompt inside the Kafka container
kubectl run -it kafka-client --image=bitnami/kafka:3.6.0 --rm --restart=Never -- bash

# List Topics
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --list

# Create Test Topic
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --create --topic test-topic --partitions 1 --replication-factor 1

# Send a Test Message
kafka-console-producer.sh --broker-list kafka-0.kafka-headless.default.svc.cluster.local:9094 --topic test-topic
>hello from kafka!
# Press `Ctrl + C` to exit the producer

# Using Console Consumer
kafka-console-consumer.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --topic
# Press `Ctrl + C` to exit the consumer
``` 

### Controlling Topic Creation (Optional)

By default, Kafka is configured to **automatically create topics** when a producer or consumer references a topic name that doesn't exist.

This can be convenient during development, but you may want to **disable it** in certain scenarios.

#### Disable Auto Topic Creation

You can prevent Kafka from creating topics automatically by setting the following environment variable in your Kafka manifest (e.g., `kafka-statefulset.yaml`):

```yaml
- name: KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE
  value: "false"
```

# Usage
Your prerequsites are installed, your environment is set up, and your Kubernetes cluster is running, you are ready to start using the application!

## Running Experiments
In the main project folder, run the following command:
```sh
python run_experiment.py
```

Follow the prompts and execute the experiments you would like. Entering 0 at any point will return you to the main menu.

### Examples
Running **Network Partition** experiment with default parameters:

```sh

```

Running **Resource Exhaustion** experiment with custom parameters:

```sh

```

## Viewing the Dashboard

### *Make sure your kubernetes cluster is up and running*

1. Open a new tab on your browser and type in "localhost:32000"
2. Username: admin, Password: admin
3. Click on "Dashboards" on the left hand side
4. Click on "Capstone"
5. Click on "Database Recovery System"

Done!

## Querying MongoDB Directly

### *Make sure your kubernetes cluster is up and running*
In your terminal...
1. Type in this command: `kubectl get pods`
2. Locate the mongodb pod and copy the name
3. Type in this command: `kubectl exec -it <mongodb pod name> -- sh`
4. Log into mongosh using this command: `mongosh -u root -p`
5. When prompted, type in the password: `root`
6. From there, you can see databases with: `show dbs`
7. Type: `use metrics_db`
8. From there, you can see collections with: `show collections`
9. To view a collection, type: `db.<collection name>.find().pretty()`
10. At this point you can enter your query

### Examples
```sh
```

## Querying MySQL Directly

### *Make sure your kubernetes cluster is up and running*
In your terminal...
1. Type in this command: `kubectl exec -it mysql-summary-records-0 -- sh`
2. Log into mongosh using this command: `mysql -u root -p`
3. When prompted, type in the password: `root`
4. From there, you can see databases with: `show databases;`
5. Type: `use summary_db;`
6. From there, you can see tables with: `show tables;`
7. To view a table, type: `select * from <table name>;`
8. At this point you can enter your query

### Examples
```sh
```

# Credits
Designed and built by Lucas Baker, Rachel Cox, Henry Hewitt, and Lukas McCain for the the February 2025 cohort of the PNC/TEKsystems Early Career SRE bootcamp. 




