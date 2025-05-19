FROM mysql:8.0

COPY replication-init.sh /docker-entrypoint-initdb.d/replication-init.sh
COPY test-log.sh /docker-entrypoint-initdb.d/test-log.sh
RUN chmod +x /docker-entrypoint-initdb.d/*.sh
