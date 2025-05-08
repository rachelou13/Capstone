from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='kafka-0.kafka.default.svc.cluster.local:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-consumer-group'
    value_deserializer=lambda m: m.decode('utf-8')
)
# Need to make the tables for the info if they are not created already so we can store the log in the databases

print("Kafka consumer is running")

consumer.subscribe(["proxy-logs", "infra-metrics"])

print("Listeninig to proxy-logs and infra-metrics")

for message in consumer:
    topic = message.topic
    value = message.value

    if topic == 'proxy-logs':
        # send to mysql in a nice way
        print()
    elif topic == 'infra-metrics':
        # send to mongodb here
        print()
    else:
        print(f"[UNKNOWN TOPIC] {topic}: {value}")


