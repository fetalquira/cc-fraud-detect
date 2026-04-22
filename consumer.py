import json
import sys
from pydantic import BaseModel, ValidationError
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# --- 1. PYDANTIC: The Bouncer ---
class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    merchant: str
    location: str
    timestamp: str

# --- 2. THE BUSINESS LOGIC ---
def is_fraud(transaction: Transaction) -> bool:
    return transaction.amount > 1000.00

# --- 3. THE EMBEDDED SCHEMA: The Blueprint ---
def format_for_jdbc(transaction_dict: dict) -> dict:
    return {
        "schema": {
            "type": "struct",
            "fields": [
                {"type": "string", "optional": False, "field": "transaction_id"},
                {"type": "string", "optional": True, "field": "user_id"},
                {"type": "double", "optional": True, "field": "amount"},
                {"type": "string", "optional": True, "field": "merchant"},
                {"type": "string", "optional": True, "field": "location"},
                {"type": "string", "optional": True, "field": "timestamp"}
            ],
            "optional": False,
            "name": "fraud_record"
        },
        "payload": transaction_dict
    }

# --- KAFKA CONFIGURATION ---
conf = {'bootstrap.servers': 'localhost:9092'}

consumer_conf = conf.copy()
consumer_conf.update({
    'group.id': 'enterprise_fraud_squad',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True 
})

consumer = Consumer(consumer_conf)
producer = Producer(conf)

inbound_topic = 'raw_transactions'
outbound_topic = 'fraud_alerts_topic'

consumer.subscribe([inbound_topic])

print("🛡️ Enterprise Decoupled Consumer activated. Scanning stream...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            else: raise KafkaException(msg.error())

        try:
            # Step A: Parse raw bytes to JSON
            raw_data = json.loads(msg.value().decode('utf-8'))
            
            # Step B: Pass data through the Pydantic Bouncer
            valid_transaction = Transaction(**raw_data)
            
            # Step C: Evaluate Business Logic
            if is_fraud(valid_transaction):
                print(f"🚨 FRAUD CAUGHT: User {valid_transaction.user_id} | ${valid_transaction.amount}")
                
                # Step D: Convert Pydantic object to dictionary, then embed the Schema
                connect_payload = format_for_jdbc(valid_transaction.model_dump())
                
                # Step E: Produce to the Outbound Topic for Kafka Connect to pick up
                producer.produce(
                    topic=outbound_topic,
                    key=valid_transaction.transaction_id.encode('utf-8'),
                    value=json.dumps(connect_payload).encode('utf-8')
                )
                producer.poll(0)
                print(f"   └── 📨 Routed clean data to outbound topic.")
            else:
                print(f"✅ Clean: User {valid_transaction.user_id} | ${valid_transaction.amount}")

        # --- THE SAFETY NETS ---
        except ValidationError as e:
            print(f"❌ PYDANTIC REJECTION: Corrupt data format.\n{e}")
            # (Day 6 target: Route this to a Dead Letter Queue)
        except json.JSONDecodeError:
            print("⚠️ FATAL: Payload is not valid JSON.")
        except Exception as e:
            print(f"⚠️ System Error: {e}")

except KeyboardInterrupt:
    print("\nShutting down gracefully...")
finally:
    consumer.close()
    producer.flush() 
    print("Clean shutdown.")