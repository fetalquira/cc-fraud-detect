import requests
import os
import json


url = 'http://localhost:8083/connectors'
headers = {
    "Content-Type": "application/json"
}
db_url = f"jdbc:postgresql://postgres:5432/{os.getenv("DB_NAME")}"
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
core_config = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "fraud_alerts_topic",
    "connection.url": db_url,
    "connection.user": db_user,
    "connection.password": db_password,
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "transaction_id",
    "auto.create": "true",
    "auto.evolve": "true",
    "table.name.format": "enterprise_fraud_alerts",
    
    # --- THE FIXES ---
    
    # 1. Override the Key Converter: Tell Kafka Connect the key is just a raw string, not JSON.
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    
    # 2. Skip Poison Pills: If a message fails to parse, log it, drop it, and move to the next one!
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
}
creation_payload = {
    "name": "postgres-enterprise-sink",
    "config": core_config
}


# Generic PUT url for Kafka Connect
# Possible options so far: pause, status, config
put_command = 'status'
generic_url = f"http://localhost:8083/connectors/postgres-enterprise-sink/{put_command}"

def kafka_options(url, data=core_config, headers=headers, options='get'):
    if options=='get':
        try:
            response = requests.get(url=url)
            status = response.json()
            print(json.dumps(status, indent=2))
        except Exception as e:
            print(f"Failed to get status: {e}")
    elif options=='post':
        try:
            response = requests.post(
                url=url,
                headers=headers,
                json=creation_payload
            )
            print(response.status_code)
        except Exception as e:
            print(f"Failed: {e}")
    elif options=='put':
        try:
            response = requests.put(
                url=url,
                json=data
            )
            print(response.status_code)
        except Exception as e:
            print(f"Failed: {e}")

# Kafka Connect via REST API
if __name__ == "__main__":
    kafka_options(generic_url, options='get', data=None)