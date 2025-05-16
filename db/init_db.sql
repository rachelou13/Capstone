CREATE DATABASE IF NOT EXISTS summary_db;
USE summary_db;

CREATE TABLE IF NOT EXISTS infra_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME,
    source VARCHAR(100),
    cpu_percent FLOAT DEFAULT 0.0,
    cpu_used FLOAT DEFAULT 0.0,
    mem_percent FLOAT DEFAULT 0.0,
    mem_used FLOAT DEFAULT 0.0,
    node_name VARCHAR(100),
    pod_name VARCHAR(100),
    pod_namespace VARCHAR(100),
    container_name VARCHAR(100) DEFAULT 'None',
    metric_level VARCHAR(20),
    INDEX idx_timestamp (timestamp),
    INDEX idx_container (container_name),
    INDEX idx_metric_level (metric_level)
);