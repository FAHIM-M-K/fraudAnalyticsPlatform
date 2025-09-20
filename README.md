# fraudAnalyticsPlatform
PySpark based fraud detection and analytics platform
Real-Time Fraud Detection Platform

An end-to-end fraud detection system that ingests live transactions, applies fraud detection rules + ML anomaly detection, and surfaces insights via a Power BI dashboard. Built for large-scale streaming analytics use cases.

###########Features

Real-time streaming using Apache Kafka + PySpark Structured Streaming.

Fraud detection rules: high-value transactions, rapid transactions, suspicious locations, unusual hours.

Machine Learning integration for anomaly detection beyond rules.

PostgreSQL integration to persist transactions and fraud alerts.

Power BI dashboard to visualize fraud rate, alerts, transaction hotspots, and patterns.

Synthetic data generation for testing (1500+ transactions, 300+ fraud alerts).

##############Project Structure
fraud_detection/
│── spark_kafka_test.py       # Main Spark Streaming job
│── sample_data.sql           # Sample SQL script to insert test data (transactions + alerts)
│── README.md                 # Project documentation

###########Tech Stack

Streaming: Apache Kafka, PySpark Structured Streaming

Database: PostgreSQL

Visualization: Power BI

Programming: Python (PySpark, Psycopg2, Faker for test data)

ML (planned): Scikit-learn anomaly detection

##########Setup Instructions
1. Clone the repository
git clone https://github.com/FAHIM-M-K/fraudAnalyticsPlatform.git
cd fraud-detection-platform

2. Start Kafka (using Kraft mode)
# Start Zookeeper (if using Zookeeper setup)
bin/zookeeper-server-start.sh config/zookeeper.properties  

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties

3. Create Kafka topic
bin/kafka-topics.sh --create --topic fraud-test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

4. Start PostgreSQL

Create database: fraud_db

Create tables:

CREATE TABLE transactions (
    transaction_id VARCHAR PRIMARY KEY,
    account_id VARCHAR,
    amount DECIMAL(10,2),
    location VARCHAR,
    transaction_time TIMESTAMP,
    ingested_at TIMESTAMP
);

CREATE TABLE fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR,
    account_id VARCHAR,
    amount DECIMAL(10,2),
    location VARCHAR,
    transaction_time TIMESTAMP,
    rule_triggered VARCHAR,
    alert_reason VARCHAR,
    created_at TIMESTAMP
);

5. Run Spark job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 spark_kafka_test.py

6. Produce test data into Kafka
bin/kafka-console-producer.sh --topic fraud-test --bootstrap-server localhost:9092


Send messages in format:

txn001|acc123|750|Dubai|2025-09-17T14:30:00
txn002|acc124|300|Paris|2025-09-17T14:35:00

7. Connect Power BI

Use PostgreSQL connector to connect to fraud_db.

Import transactions and fraud_alerts tables.

Build dashboard with metrics: Fraud Rate %, Fraud Alerts Trend, Fraud by Location, Fraud by Rule.

8. Connect to powerbi for visulizations
