FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker/test-app/test-service.py .

CMD ["python", "test-service.py"]

