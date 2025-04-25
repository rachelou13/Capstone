from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='log-processor'
)

print("Kafka consumer started. Waiting for messages...")

for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
