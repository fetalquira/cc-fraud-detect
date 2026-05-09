from pyspark.sql import SparkSession
from pyspark.sql.functions import from_avro, col, expr, current_timestamp
from pyspark.sql.column import Column

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
# We convert the binary 'value' column into a struct using the Avro schema
decoded_df = raw_df.select(
    from_avro(col("value"), json_schema).alias("data")
).select("data.*")

# 5. The Fraud Logic
# We add a 'is_fraud' flag and a processing timestamp
final_df = decoded_df.withColumn("is_fraud", col("amount") > 10000) \
                     .withColumn("processed_at", current_timestamp())

# 6. Write the Result back to Kafka
# This 'clean_transactions' topic will be picked up by our Postgres Sink
query = final_df.selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "clean_transactions") \
    .option("checkpointLocation", "/tmp/spark_checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()