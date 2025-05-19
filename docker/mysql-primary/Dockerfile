FROM mysql:8.0

# Add an init SQL script to create the replica user
COPY init-replica-user.sql /docker-entrypoint-initdb.d/init-replica-user.sql
