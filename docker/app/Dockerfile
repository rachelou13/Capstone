FROM python:3.11-slim

WORKDIR /app

COPY mysql-proxy-server.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

EXPOSE 3307

CMD ["python", "mysql-proxy-server.py"]
