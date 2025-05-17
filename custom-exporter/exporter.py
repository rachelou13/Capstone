from flask import Flask, Response
import mysql.connector
from pymongo import MongoClient
import os

from datetime import datetime, timezone

app = Flask(__name__)

@app.route("/metrics")
def metrics():
    lines = []

    # === MySQL (container metrics only) ===
    try:
        mysql_conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "mysql-summary-records"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DB", "summary_db"),
            connection_timeout=3
        )
        cursor = mysql_conn.cursor(dictionary=True)

        # === 1. Check if mysql container is up ===
        cursor.execute("""
            SELECT cpu_percent
            FROM infra_metrics
            WHERE metric_level = 'container'
              AND pod_name = 'mysql-primary-0'
              AND container_name = 'mysql'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row and row["cpu_percent"] is not None:
            lines.append("mysql_primary_up 1")
        else:
            lines.append("mysql_primary_up 0")

        # === 2. Summary stats for mysql container ===
        cursor.execute("""
            SELECT AVG(cpu_percent) AS avg_cpu, AVG(mem_percent) AS avg_mem, COUNT(*) as total
            FROM infra_metrics
            WHERE metric_level = 'node'
              AND pod_name = 'mysql'
        """)
        row = cursor.fetchone()
        if row:
            lines.append(f'infra_avg_cpu_percent {row["avg_cpu"] or 0:.2f}')
            lines.append(f'infra_avg_mem_percent {row["avg_mem"] or 0:.2f}')
            lines.append(f'infra_metric_total_scrapes {row["total"] or 0}')

        # === 3. Emit latest container metrics (or 0 if missing) ===
        container_targets = [
            {"pod": "mysql-primary-0", "container": "mysql"},
        ]

        for target in container_targets:
            pod = target["pod"]
            container = target["container"]

            cursor.execute("""
                SELECT *
                FROM infra_metrics
                WHERE metric_level = 'node'
                  AND pod_name = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (pod,))

            row = cursor.fetchone()
            labels = f'pod="{pod}"'

            if row and row["cpu_percent"] is not None:
                lines.append(f'infra_cpu_usage_percent{{{labels}}} {row["cpu_percent"]}')
            else:
                lines.append(f'infra_cpu_usage_percent{{{labels}}} 0')

            if row and row["cpu_used"] is not None:
                lines.append(f'infra_cpu_usage_absolute{{{labels}}} {row["cpu_used"]}')
            else:
                lines.append(f'infra_cpu_usage_absolute{{{labels}}} 0')


            if row and row["mem_percent"] is not None:
                lines.append(f'infra_mem_usage_percent{{{labels}}} {row["mem_percent"]}')
            else:
                lines.append(f'infra_mem_usage_percent{{{labels}}} 0')

            if row and row["mem_used"] is not None:
                mem_used_gb = row["mem_used"] / 1024 / 1024 / 1024
                lines.append(f'infra_mem_usage_absolute{{{labels}}} {mem_used_gb:.2f}')
            else:
                lines.append(f'infra_mem_usage_absolute{{{labels}}} 0')

        cursor.close()
        mysql_conn.close()

    except Exception as e:
        lines.append("mysql_primary_up 0")
        lines.append(f"# MySQL error: {str(e)}")

    # === MongoDB: chaos_events + proxy_logs ===
    try:
        mongo_client = MongoClient("mongodb://root:root@mongodb-service:27017/", serverSelectionTimeoutMS=3000)
        db = mongo_client["metrics_db"]

        # CHAOS EVENTS
        chaos_collection = db["chaos_events"]
        total_chaos = chaos_collection.count_documents({}, maxTimeMS=2000)
        lines.append(f'chaos_events_total {total_chaos}')

        pipeline = [{"$group": {"_id": "$event_type", "count": {"$sum": 1}}}]
        for doc in chaos_collection.aggregate(pipeline):
            event_type = doc["_id"]
            count = doc["count"]
            lines.append(f'chaos_event_count{{event_type="{event_type}"}} {count}')

        pipeline = [
            {"$match": {"event_type": "start"}},
            {"$group": {"_id": "$source", "count": {"$sum": 1}}}
        ]
        for doc in chaos_collection.aggregate(pipeline):
            chaos_type = doc["_id"]
            count = doc["count"]
            lines.append(f'chaos_events_total_by_type{{chaos_type="{chaos_type}"}} {count}')

        latest_start = chaos_collection.find_one({"event_type": "start"}, sort=[("timestamp", -1)])
        if latest_start and "timestamp" in latest_start:
            last_ts = latest_start["timestamp"]
            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts)
            seconds_since = (datetime.now(timezone.utc) - last_ts).total_seconds()
            lines.append(f'seconds_since_last_chaos_event {seconds_since:.0f}')

        latest_event = chaos_collection.find_one(
            {"event_type": {"$in": ["start", "end"]}},
            sort=[("timestamp", -1)]
        )
        is_running = 1 if latest_event and latest_event.get("event_type") == "start" else 0
        lines.append(f'chaos_experiment_running {is_running}')

        # PROXY LOGS
        proxy_collection = db["proxy_logs"]
        for event in ["recv_failed", "broken_pipe", "connection_accepted"]:
            count = proxy_collection.count_documents({"event": event})
            lines.append(f'proxy_log_errors_total{{event="{event}"}} {count}')

    except Exception as e:
        lines.append(f'# MongoDB error: {str(e)}')

    return Response("\n".join(lines), mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
