import requests
from yarl import URL
import os
import json
import argparse


# URL & Header
base = URL('http://localhost:8083/connectors')
headers = {
    "Content-Type": "application/json"
}

# DB Credentials
db_url = f"jdbc:postgresql://postgres:5432/{os.getenv("DB_NAME")}"
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Config Variables
topics = 'clean_transactions'
table_name = 'processed_transactions'
dlq_topic = 'dlq_database_errors'
SINK_NAME = 'postgres-enterprise-sink'

# THE CONFIGURATION
core_config = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": topics,
    "connection.url": db_url,
    "connection.user": db_user,
    "connection.password": db_password,
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "transaction_id",
    "auto.create": "true",
    "auto.evolve": "true",
    "table.name.format": table_name,

    # 1. Add the JSON Value Converter Overrides
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    # 1. Override the Key Converter: Tell Kafka Connect the key is just a raw string, not JSON.
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    
    # --- 2. THE INFRASTRUCTURE DLQ ---
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": dlq_topic,
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
}

# Creation Payload for POST
creation_payload = {
    "name": SINK_NAME,
    "config": core_config
}

# Possible commands: POST, PUT (pause), DELETE, GET (status)
def kafka_options():
    parser = argparse.ArgumentParser()
    
    # Add all the default variables
    parser.add_argument("--url", default=base)
    parser.add_argument("--sink_name", default=SINK_NAME)
    parser.add_argument("--config", default=core_config)
    parser.add_argument("--creation_payload", default=creation_payload)
    parser.add_argument("--headers", default=headers)
    parser.add_argument("--options", default='get')

    args = parser.parse_args()

    if args.options=='get':
        try:
            response = requests.get(url = args.url / args.sink_name / "status")
            status = response.json()
            print(json.dumps(status, indent=2))
        except Exception as e:
            print(f"Failed to get status: {e}")
    elif args.options=='post':
        try:
            response = requests.post(
                url = args.url,
                headers = args.headers,
                json = args.creation_payload # Make sure to use creation_payload instead of core_config on default
            )
            print(response.status_code)
            print(response.text)
        except Exception as e:
            print(f"Failed: {e}")
    elif args.options=='put':
        try:
            response = requests.put(
                url = args.url / args.sink_name / "config",
                json = args.config
            )
            print(response.status_code)
            print(response.text)
        except Exception as e:
            print(f"Failed: {e}")
    elif args.options == 'delete':
        try:
            response = requests.delete(
                url = args.url / args.sink_name
            )
            print(response.status_code)
            print(response.text)
        except Exception as e:
            print(f"Failed: {e}")

# Kafka Connect via REST API
if __name__ == "__main__":
    kafka_options()