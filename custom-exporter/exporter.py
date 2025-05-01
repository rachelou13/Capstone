from flask import Flask, Response
import mysql.connector
from pymongo import MongoClient
import os

app = Flask(__name__)

@app.route("/metrics")
def metrics():
    lines = []

    # Query MySQL for latest infra_metrics
    try:
        mysql_conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "mysql-summary-records"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DB", "summary_db")
        )
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT rto_ms, experiment FROM infra_metrics ORDER BY end_time DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            lines.append(f'recovery_rto_ms{{experiment="{row["experiment"]}"}} {row["rto_ms"]}')
        cursor.close()
        mysql_conn.close()
    except Exception as e:
        lines.append(f'# MySQL error: {str(e)}')

    # Query MongoDB for chaos event count
    try:
        mongo_client = MongoClient("mongodb://root:password@mongodb:27017/admin")
        db = mongo_client.chaos_db
        count = db.chaos_events.count_documents({})
        lines.append(f'chaos_events_total {count}')
    except Exception as e:
        lines.append(f'# MongoDB error: {str(e)}')

    return Response("\n".join(lines) + "\n", mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
