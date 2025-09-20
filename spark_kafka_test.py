# import traceback
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, split, current_timestamp, lit

# # Use your conda env python
# os.environ["PYSPARK_PYTHON"] = r"C:\Users\fahim\miniconda3\envs\fraudstream\python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\fahim\miniconda3\envs\fraudstream\python.exe"

# # ---------------- CONFIG ----------------
# JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0"
# CHECKPOINT_DIR = "file:///C:/tmp/spark-checkpoints/fraud-pipeline"
# KAFKA_BOOTSTRAP = "localhost:9092"
# KAFKA_TOPIC = "fraud-test"

# POSTGRES_URL = "jdbc:postgresql://localhost:5432/fraud_db"
# POSTGRES_USER = "postgres"
# POSTGRES_PASSWORD = "Cherry3,ontop"
# TRANSACTIONS_TABLE = "transactions"
# FRAUD_TABLE = "fraud_alerts"
# # ----------------------------------------

# spark = SparkSession.builder \
#     .appName("FraudPipeline") \
#     .master("local[*]") \
#     .config("spark.jars.packages", JARS_PACKAGES) \
#     .config("spark.hadoop.io.nativeio.nativeio.use.windows", "false") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Read from Kafka
# raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Parse directly from value (no raw_value left behind)
# # Expected message format: txn_id|account_id|amount|location|timestamp
# parsed = raw.selectExpr("CAST(value AS STRING) AS value") \
#     .withColumn("transaction_id", split(col("value"), "\\|").getItem(0)) \
#     .withColumn("account_id", split(col("value"), "\\|").getItem(1)) \
#     .withColumn("amount", split(col("value"), "\\|").getItem(2).cast("double")) \
#     .withColumn("location", split(col("value"), "\\|").getItem(3)) \
#     .withColumn("transaction_time", split(col("value"), "\\|").getItem(4).cast("timestamp")) \
#     .drop("value")

# # Define fraud rule(s)
# # Rule A: amount > 500
# alerts = parsed.filter(col("amount") > 500) \
#     .withColumn("rule_triggered", lit("HIGH_AMOUNT")) \
#     .withColumn("alert_reason", lit("Transaction amount above $500")) \
#     .withColumn("created_at", current_timestamp())

# # foreachBatch writes both tables within same batch - safer for atomicity at app-level
# def write_batch(batch_df, batch_id):
#     try:
#         if batch_df.rdd.isEmpty():
#             print(f"[foreachBatch] batch {batch_id} empty")
#             return

#         # Persist if reusing
#         batch_df.persist()

#         # 1) Write transactions (all rows)
#         tx_df = batch_df.select(
#             "transaction_id", "account_id", "amount", "location", "transaction_time"
#         ).withColumn("ingested_at", current_timestamp())

#         print(f"[foreachBatch] batch {batch_id} - writing {tx_df.count()} transactions")
#         tx_df.write \
#             .format("jdbc") \
#             .option("url", POSTGRES_URL) \
#             .option("dbtable", TRANSACTIONS_TABLE) \
#             .option("user", POSTGRES_USER) \
#             .option("password", POSTGRES_PASSWORD) \
#             .option("driver", "org.postgresql.Driver") \
#             .mode("append") \
#             .save()

#         # 2) Write fraud alerts (subset)
#         fraud_df = tx_df.filter(col("amount") > 500) \
#             .select("transaction_id", "account_id", "amount", "location", "transaction_time") \
#             .withColumn("rule_triggered", lit("HIGH_AMOUNT")) \
#             .withColumn("alert_reason", lit("Transaction amount above $500")) \
#             .withColumn("created_at", current_timestamp())

#         if fraud_df.rdd.isEmpty():
#             print(f"[foreachBatch] batch {batch_id} - no fraud rows")
#         else:
#             print(f"[foreachBatch] batch {batch_id} - writing {fraud_df.count()} fraud alerts")
#             fraud_df.write \
#                 .format("jdbc") \
#                 .option("url", POSTGRES_URL) \
#                 .option("dbtable", FRAUD_TABLE) \
#                 .option("user", POSTGRES_USER) \
#                 .option("password", POSTGRES_PASSWORD) \
#                 .option("driver", "org.postgresql.Driver") \
#                 .mode("append") \
#                 .save()

#         batch_df.unpersist()
#         print(f"[foreachBatch] batch {batch_id} done")
#     except Exception as e:
#         print(f"[foreachBatch] ERROR batch {batch_id}: {e}")
#         traceback.print_exc()

# # NOTE: use alerts.writeStream or use parsed.writeStream? We use parsed but call foreachBatch with parsed so we have full rows.
# query = parsed.writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_batch) \
#     .option("checkpointLocation", CHECKPOINT_DIR) \
#     .start()

# query.awaitTermination()




#-----------------------OLD---------#############
####################################/////////////
####################################/////////////
####################################////////////
####################################////////////
####################################/////////////
####################################//////////////
####################################/////////////
#----------------------NEW----------##############



import traceback
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, current_timestamp, lit, window, count, hour
)

# ---------------- ENV CONFIG ----------------
os.environ["PYSPARK_PYTHON"] = r"C:\Users\fahim\miniconda3\envs\fraudstream\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\fahim\miniconda3\envs\fraudstream\python.exe"

# ---------------- CONFIG ----------------
JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0"
CHECKPOINT_DIR = "file:///C:/tmp/spark-checkpoints/fraud-pipeline"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "fraud-test"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/fraud_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "Cherry3,ontop"
TRANSACTIONS_TABLE = "transactions"
FRAUD_TABLE = "fraud_alerts"
# ----------------------------------------

spark = SparkSession.builder \
    .appName("FraudPipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", JARS_PACKAGES) \
    .config("spark.hadoop.io.nativeio.nativeio.use.windows", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------- READ KAFKA ----------------
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Expected Kafka message format:
# txn_id|account_id|amount|location|timestamp
parsed = raw.selectExpr("CAST(value AS STRING) AS value") \
    .withColumn("transaction_id", split(col("value"), "\\|").getItem(0)) \
    .withColumn("account_id", split(col("value"), "\\|").getItem(1)) \
    .withColumn("amount", split(col("value"), "\\|").getItem(2).cast("double")) \
    .withColumn("location", split(col("value"), "\\|").getItem(3)) \
    .withColumn("transaction_time", split(col("value"), "\\|").getItem(4).cast("timestamp")) \
    .drop("value")

# ---------------- FRAUD RULES ----------------
def generate_fraud_rules(tx_df):
    """Return fraud alerts DataFrame applying all rules"""

    # Rule A: High Amount (> 500)
    rule_a = tx_df.filter(col("amount") > 500) \
        .withColumn("rule_triggered", lit("HIGH_AMOUNT")) \
        .withColumn("alert_reason", lit("Transaction amount above $500"))

    # Rule B: Rapid Transactions per Account (more than 3 per minute)
    rule_b = tx_df \
        .groupBy(
            col("account_id"),
            window(col("transaction_time"), "1 minute")
        ).agg(count("*").alias("txn_count")) \
        .filter(col("txn_count") > 3) \
        .select(
            col("account_id"),
            lit(None).alias("transaction_id"),
            lit(None).alias("amount"),
            lit(None).alias("location"),
            col("window.start").alias("transaction_time"),
            lit("RAPID_TXNS").alias("rule_triggered"),
            lit("More than 3 txns in 1 min").alias("alert_reason")
        )

    # Rule C: Suspicious Location
    trusted_locations = ["Dubai", "Abu Dhabi"]
    rule_c = tx_df.filter(~col("location").isin(trusted_locations)) \
        .withColumn("rule_triggered", lit("SUSPICIOUS_LOCATION")) \
        .withColumn("alert_reason", lit("Transaction outside trusted locations"))

    # Rule D: Night Transactions (00:00–05:00)
    rule_d = tx_df.filter((hour(col("transaction_time")) >= 0) & (hour(col("transaction_time")) < 5)) \
        .withColumn("rule_triggered", lit("NIGHT_TXN")) \
        .withColumn("alert_reason", lit("Transaction at unusual hour"))

    # Union all alerts together
    return rule_a.unionByName(rule_c).unionByName(rule_d).unionByName(rule_b)
# ------------------------------------------------

# ---------------- WRITE BATCH ----------------
def write_batch(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            print(f"[foreachBatch] batch {batch_id} empty")
            return

        # Persist if reusing
        batch_df.persist()

        # 1) Write transactions (all rows)
        tx_df = batch_df.select(
            "transaction_id", "account_id", "amount", "location", "transaction_time"
        ).withColumn("ingested_at", current_timestamp())

        print(f"[foreachBatch] batch {batch_id} - writing {tx_df.count()} transactions")
        tx_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", TRANSACTIONS_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        # 2) Write fraud alerts (drop ingested_at before rules)
        fraud_df = generate_fraud_rules(tx_df.drop("ingested_at")) \
            .withColumn("created_at", current_timestamp())

        if fraud_df.rdd.isEmpty():
            print(f"[foreachBatch] batch {batch_id} - no fraud rows")
        else:
            print(f"[foreachBatch] batch {batch_id} - writing {fraud_df.count()} fraud alerts")
            fraud_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", FRAUD_TABLE) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

        batch_df.unpersist()
        print(f"[foreachBatch] batch {batch_id} done")

    except Exception as e:
        print(f"[foreachBatch] ERROR batch {batch_id}: {e}")
        traceback.print_exc()

# ---------------- STREAM START ----------------
query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

query.awaitTermination()
