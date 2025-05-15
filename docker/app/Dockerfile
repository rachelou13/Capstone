FROM python:3.11-slim

WORKDIR /app

COPY mysql-proxy-server.py .
COPY requirements.txt .

#Install iptables, netcat and other networking tools for running/validating network partition experiment
RUN apt-get update && \
    apt-get install -y netcat-openbsd curl telnet iptables && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

EXPOSE 3307

CMD ["python", "mysql-proxy-server.py"]