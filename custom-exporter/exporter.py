from flask import Flask, Response
import mysql.connector
from pymongo import MongoClient
import os

app = Flask(__name__)

@app.route("/metrics")
def metrics():
    lines = []

    # === MySQL: infra_metrics ===
    try:
        mysql_conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "mysql-summary-records"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DB", "summary_db"),
            connection_timeout=3
        )
        cursor = mysql_conn.cursor(dictionary=True)

        # Summary stats
        cursor.execute("SELECT AVG(cpu_percent) AS avg_cpu, AVG(mem_percent) AS avg_mem, COUNT(*) as total FROM infra_metrics")
        row = cursor.fetchone()
        if row:
            lines.append(f'infra_avg_cpu_percent {row["avg_cpu"] or 0:.2f}')
            lines.append(f'infra_avg_mem_percent {row["avg_mem"] or 0:.2f}')
            lines.append(f'infra_metric_total_scrapes {row["total"] or 0}')

        # Latest experiment detected
        cursor.execute("SELECT experiment_detected FROM infra_metrics ORDER BY timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            is_running = int(row["experiment_detected"])
            lines.append(f'chaos_experiment_running {is_running}')

        # Raw metrics
        cursor.execute("SELECT * FROM infra_metrics WHERE metric_level = 'node' ORDER BY timestamp DESC LIMIT 1")
        for row in cursor.fetchall():
            pod = row.get("pod_name", "unknown")
            node = row.get("node_name", "unknown")
            namespace = row.get("pod_namespace", "default")

            cpu_percent = row.get("cpu_percent")
            cpu_used = row.get("cpu_used")
            mem_percent = row.get("mem_percent")
            mem_used = row.get("mem_used")

            if cpu_percent is not None:
                lines.append(f'infra_cpu_usage_percent{{pod="{pod}", node="{node}", namespace="{namespace}"}} {cpu_percent}')
            if cpu_used is not None:
                lines.append(f'infra_cpu_usage_absolute{{pod="{pod}", node="{node}", namespace="{namespace}"}} {cpu_used}')
            if mem_percent is not None:
                lines.append(f'infra_mem_usage_percent{{pod="{pod}", node="{node}", namespace="{namespace}"}} {mem_percent}')
            if mem_used is not None:
                lines.append(f'infra_mem_usage_absolute{{pod="{pod}", node="{node}", namespace="{namespace}"}} {mem_used}')

        cursor.close()
        mysql_conn.close()
    except Exception as e:
        lines.append(f'# MySQL error: {str(e)}')

    # === MongoDB: chaos_events + proxy_logs ===
    try:
        mongo_client = MongoClient("mongodb://root:root@mongodb-service:27017/", serverSelectionTimeoutMS=3000)
        db = mongo_client["metrics_db"]

        # CHAOS EVENTS
        chaos_collection = db["chaos_events"]

        # Count total chaos events
        total_chaos = chaos_collection.count_documents({}, maxTimeMS=2000)
        lines.append(f'chaos_events_total {total_chaos}')

        # Count chaos events by event_type
        pipeline = [{"$group": {"_id": "$event_type", "count": {"$sum": 1}}}]
        for doc in chaos_collection.aggregate(pipeline):
            event_type = doc["_id"]
            count = doc["count"]
            lines.append(f'chaos_event_count{{event_type="{event_type}"}} {count}')

        # Count by chaos_type
        pipeline = [
            {"$match": {"event_type": "start"}},
            {"$group": {"_id": "$source", "count": {"$sum": 1}}}
            ]
        for doc in chaos_collection.aggregate(pipeline):
            chaos_type = doc["_id"]
            count = doc["count"]
            lines.append(f'chaos_events_total_by_type{{chaos_type="{chaos_type}"}} {count}')

        # Time since last chaos event
        from datetime import datetime, timezone
        latest_event = chaos_collection.find_one({"event_type": "start"}, sort=[("timestamp", -1)])
        if latest_event and "timestamp" in latest_event:
            last_ts = latest_event["timestamp"]
            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts)
            seconds_since = (datetime.now(timezone.utc) - last_ts).total_seconds()
            lines.append(f'seconds_since_last_chaos_event {seconds_since:.0f}')

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
