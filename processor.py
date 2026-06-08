from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.column import Column
import json

# 1. Initialize the Session
# We need the Spark-Sql-Kafka and Spark-Avro packages to talk to our stack
spark = SparkSession.builder \
    .appName("FraudDetectionEngine") \
    .getOrCreate()

# 2. Read the Raw Stream
# We connect to the INTERNAL docker network address 'kafka:29092'
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Handle the Schema (The "Successor's" Secret)
# In production, we'd fetch the schema from the registry. 
# For now, we define the schema string to match your transaction_schema.avsc
json_schema = """
{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "card_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "vendor_id", "type": "string"},
    {"name": "transaction_time", "type": "string"}
  ]
}
"""

# 4. Transform: Binary -> Structured -> Insights
# We add an option to handle parsing safely. We use 'PERMISSIVE' inside a 
# specialized structure so we can capture the corrupt data instead of crashing.
avro_options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "corrupt_data"}

# TRICK: If your producer uses Confluent Avro, it prepends 5 magic bytes.
# We slice off the first 5 bytes using expr("substring(value, 6)") to get the pure Avro record.
# If your producer sends raw Avro WITHOUT Confluent, change col("pure_bytes") back to col("value").
decoded_df = raw_df.withColumn("pure_bytes", expr("substring(value, 6)")) \
    .select(from_avro(col("pure_bytes"), json_schema, avro_options).alias("data")) \
    .select("data.*")

# 5. The Enterprise Error Routing Logic
# If a row is corrupt, Spark's permissive engine will fail to unpack it, 
# resulting in critical schema fields like transaction_id becoming null.
clean_df = decoded_df.filter(col("transaction_id").isNotNull()) \
    .withColumn("is_fraud", col("amount") > 10000) \
    .withColumn("processed_at", current_timestamp())

# OPTIONAL DLQ SINK: You can capture bad rows here to route to a dead-letter topic
bad_df = decoded_df.filter(col("transaction_id").isNull())

# Define the exact blueprint Kafka Connect needs to auto-create the table
connect_schema = {
    "type": "struct",
    "name": "record",
    "optional": False,
    "fields": [
        {"type": "string", "optional": True, "field": "transaction_id"},
        {"type": "string", "optional": True, "field": "card_id"},
        {"type": "double", "optional": True, "field": "amount"},
        {"type": "string", "optional": True, "field": "vendor_id"},
        {"type": "string", "optional": True, "field": "transaction_time"},
        {"type": "boolean", "optional": True, "field": "is_fraud"},
        {"type": "string", "optional": True, "field": "processed_at"}
    ]
}
schema_str = json.dumps(connect_schema)

# Cast the timestamp to a string so it aligns cleanly with the JSON schema
clean_df = clean_df.withColumn("processed_at", col("processed_at").cast("string"))

# 6A. Write the Clean Stream to production
query_clean = clean_df.selectExpr(
    "CAST(transaction_id AS STRING) AS key",
    f"concat('{{\"schema\": {schema_str}, \"payload\": ', to_json(struct(*)), '}}') AS value"
) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "clean_transactions") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/clean") \
    .outputMode("append") \
    .start()

# 6B. Write the Bad Stream to the DLQ
# Even if transaction_id is null (key becomes null), Kafka accepts it and routes it safely.
query_dlq = bad_df.selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "dlq_processing_errors") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/dlq") \
    .outputMode("append") \
    .start()

# 7. The Master Loop
# Because we have two active streams, we don't wait on one specific query.
# We tell the Spark Session to wait for ANY active stream to terminate.
spark.streams.awaitAnyTermination()