import uuid
import json
import time
from datetime import datetime, timezone
from faker import Faker
from pydantic import BaseModel, ValidationError

# The Enterprise Upgrades
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

fake = Faker()

# ==========================================
# 1. THE APPLICATION CONTRACT (Pydantic)
# ==========================================
class Transaction(BaseModel):
    transaction_id: str
    credit_card_num: str
    user_id: str
    amount: float
    merchant: str
    location: str
    timestamp: str

# ==========================================
# 2. THE INFRASTRUCTURE CONTRACT (Avro)
# ==========================================
# Read the Avro schema file you created
with open('transaction_schema.avsc', 'r') as f:
    schema_str = f.read()

# Connect to the Schema Registry we just spun up
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Initialize the Avro Serializer 
# (This talks to the registry and converts our data into binary)
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
string_serializer = StringSerializer('utf_8')

# ==========================================
# 3. THE PRODUCER ENGINE
# ==========================================
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': string_serializer,       # Keys remain readable strings
    'value.serializer': avro_serializer        # Values become highly compressed binary
}

producer = SerializingProducer(producer_conf)

def delivery_report(err, msg):
    """Callback triggered when a message is successfully delivered or fails."""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ [AVRO ENFORCED] Record {msg.key().decode('utf-8')} produced to {msg.topic()} partition [{msg.partition()}]")

def generate_transaction():
    """Generates synthetic data."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "credit_card_num": str(fake.credit_card_number()),
        "user_id": f"U-{fake.random_int(min=1000, max=9999)}",
        "amount": round(fake.random.uniform(5.0, 1000.0), 2),
        "merchant": fake.company(),
        "location": fake.city(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

print("🛡️ Enterprise Producer Active. Generating binary Avro streams...")

try:
    while True:
        raw_data = generate_transaction()
        
        try:
            # 1. Application Boundary Check (Pydantic)
            validated_txn = Transaction(**raw_data)
            
            # 2. Infrastructure Boundary Check (Avro Serialization)
            producer.produce(
                topic='raw_transactions',
                key=validated_txn.transaction_id,
                value=validated_txn.model_dump(), # The Serializer automatically handles this!
                on_delivery=delivery_report
            )
            
            # Force the message out immediately for testing
            producer.poll(0) 
            
        except ValidationError as e:
            # --- THE DLQ UPGRADE ---
            error_payload = {
                "raw_payload": raw_data,
                "error_details": e.errors(), # Pydantic gives us a clean list of errors
                "rejected_at": time.time()
            }
            
            print(f"⚠️  ROUTING TO DLQ: {raw_data.get('transaction_id', 'unknown')}")
            
            # We send this as raw JSON because if it failed Pydantic, 
            # it might not fit our strict Avro schema anyway.
            # Use a basic Producer or a separate topic for raw triage.
            producer.produce(
                topic='validation_errors',
                key=raw_data.get('transaction_id', str(uuid.uuid4())),
                value=json.dumps(error_payload).encode('utf-8'), # Raw bytes
                on_delivery=delivery_report
            )
            producer.poll(0)
            
        time.sleep(1) # Throttle to 1 message per second for visual monitoring

except KeyboardInterrupt:
    print("\n🛑 Shutting down producer...")
finally:
    producer.flush()