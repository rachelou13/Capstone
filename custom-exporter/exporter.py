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
            database=os.getenv("MYSQL_DB", "summary_db")
        )
        cursor = mysql_conn.cursor(dictionary=True)

        cursor.execute("SELECT AVG(cpu_percent) AS avg_cpu, AVG(mem_percent) AS avg_mem, COUNT(*) as total FROM infra_metrics")
        row = cursor.fetchone()
        if row:
            lines.append(f'infra_avg_cpu_percent {row["avg_cpu"]:.2f}')
            lines.append(f'infra_avg_mem_percent {row["avg_mem"]:.2f}')
            lines.append(f'infra_metric_total_scrapes {row["total"]}')
        
        cursor.close()
        mysql_conn.close()
    except Exception as e:
        lines.append(f'# MySQL error: {str(e)}')

    # === MongoDB: chaos_events ===
    try:
        mongo_client = MongoClient("mongodb://root:root@mongodb:27017/")
        db = mongo_client["metrics_db"]
        collection = db["chaos_events"]

        # Count total chaos events
        total_chaos = collection.count_documents({})
        lines.append(f'chaos_events_total {total_chaos}')
    except Exception as e:
        lines.append(f'# MongoDB error: {str(e)}')

    return Response("\n".join(lines), mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
