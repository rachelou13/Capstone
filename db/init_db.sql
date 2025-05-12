CREATE DATABASE IF NOT EXISTS summary_db;
USE summary_db;

CREATE TABLE IF NOT EXISTS infra_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME,
    source VARCHAR(100),
    cpu_percent FLOAT,
    cpu_used FLOAT,
    mem_percent FLOAT,
    mem_used FLOAT,
    node_name VARCHAR(100),
    pod_name VARCHAR(100),
    pod_namespace VARCHAR(100)
);
