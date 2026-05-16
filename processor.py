from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.avro.functions import from_avro
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

# 6. Write the Result back to Kafka (Clean Channel)
query = clean_df.selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "clean_transactions") \
    .option("checkpointLocation", "/tmp/spark_checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()