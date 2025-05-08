# Kafka KRaft Mode Setup

##  Preconditions

Before deploying Kafka to the local Kubernetes cluster, make sure:

### 1. Minikube Is Running

```bash
minikube status
# If not running
minikube start
```

### 2. Kubernetes Namespace is Created (if not already)

```bash
kubectl create namespace staging
```


---

### ✅ Now, To Add It:

1. Run:
   ```bash
   nano docs/kafka-setup.md

---

##  Deploy Kafka in KRaft Mode

### Step 1: Apply Kafka Headless Service, Kafka NodePort Service

```bash
kubectl apply -f k8s/services/kafka-headless.yaml
kubectl apply -f k8s/services/kafka-nodeport.yaml
```

### Step 2: Apply the Kafka StatefulSet (KRaft mode)

```bash
kubectl apply -f k8s/statefulsets/kafka-statefulset.yaml
```

### Step 3: Watch Pod Startup

```bash
kubectl get pods -l app=kafka -w
```

Wait until the pod enters the `Running` state. It may go through a few restarts during setup.

---

##  Validate Kafka is Running

### Step 4: Launch Kafka Client Pod

```bash
kubectl run -it kafka-client --image=bitnami/kafka:3.6.0 --rm --restart=Never -- bash
```

You should get a shell prompt inside the Kafka container.

### Step 5: List Topics

```bash
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --list
```

Expected output (example):

```
__consumer_offsets
logs
```

### Step 6: Create Test Topic

```bash
kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --create --topic test-topic --partitions 1 --replication-factor 1
```

### Step 7: Send a Test Message

```bash
kafka-console-producer.sh --broker-list kafka-0.kafka-headless.default.svc.cluster.local:9092 --topic test-topic
>hello from kafka!
>another test message
```

Press `Ctrl + C` to exit the producer.

# Step 8: Using Console Consumer
```sh
kafka-console-consumer.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9094 --topic
``` 

---

##  Cleanup

If you ever need to wipe the Kafka StatefulSet:

```bash
kubectl delete statefulset kafka --cascade=orphan
kubectl delete pod kafka-0
```

---

##  You're Done!

Kafka in KRaft mode should now be running and accepting messages in your local Minikube Kubernetes environment. 

---

## Controlling Topic Creation (Optional)

By default, Kafka is configured to **automatically create topics** when a producer or consumer references a topic name that doesn't exist.

This can be convenient during development, but you may want to **disable it** in certain scenarios.

### 🔧 To Disable Auto Topic Creation

You can prevent Kafka from creating topics automatically by setting the following environment variable in your Kafka manifest (e.g., `kafka-statefulset.yaml`):

```yaml
- name: KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE
  value: "false"


