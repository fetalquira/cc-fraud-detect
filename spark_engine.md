# Spark Engine (Initialization)

1. Run the following command after initializing docker for `docker-compose.yml`.
```bash
docker exec -it spark-master /bin/bash
```
2. Once spark-master terminal shows up, running the following command:
```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1 \
  /opt/spark-apps/processor.py
```