from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='kafka-0.kafka.default.svc.cluster.local:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-consumer-group'
)

print("✅ Kafka consumer is running and waiting for messages...")

for message in consumer:
    print(f"📩 Received message: {message.value.decode()}")

