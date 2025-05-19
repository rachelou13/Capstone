#!/bin/bash
set -e

echo "[replication-init] SCRIPT STARTED"

until mysql -h mysql-primary -uroot -p"$MYSQL_ROOT_PASSWORD" -e "SELECT 1" &>/dev/null; do
  echo "[replication-init] Waiting for primary..."
  sleep 3
done

echo "[replication-init] Connecting and starting replication..."

mysql -uroot -p"$MYSQL_ROOT_PASSWORD" <<EOF
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='mysql-primary',
  SOURCE_USER='replica_user',
  SOURCE_PASSWORD='replica_password',
  SOURCE_AUTO_POSITION=1;
START REPLICA;
EOF

echo "[replication-init] SCRIPT COMPLETED"
