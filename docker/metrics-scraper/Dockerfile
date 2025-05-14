FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DEFAULT_KAFKA_BROKERS="kafka-0.kafka-headless.default.svc.cluster.local:9094" \
    DEFAULT_KAFKA_TOPIC="infra-metrics"

COPY python/data_scripts/metrics_scraper.py .

CMD ["python", "metrics_scraper.py"]