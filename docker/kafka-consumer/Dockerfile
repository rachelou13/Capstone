FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker/kafka-consumer/kafka_consumer.py .

CMD ["python", "./kafka_consumer.py"]