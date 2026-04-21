import json
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

# Initialize Faker for synthetic data
fake = Faker()

# Kafka Configuration
# We connect to the 'localhost' port we exposed in docker-compose.yml
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'fraud-sensor-producer'
}

# Initialize the Producer
producer = Producer(conf)
topic = 'raw_transactions'

def delivery_report(err, msg):
    """
    Data King Architecture: Always implement a callback.
    Kafka is asynchronous. This tells us if the message actually hit the disk or failed.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered transaction to {msg.topic()} from user {msg.key()} [Partition: {msg.partition()}]")

def generate_transaction():
    """Generates a synthetic JSON credit card transaction."""
    # We simulate a mix of normal and high-value transactions for the fraud logic later
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"U-{fake.random_int(min=1000, max=1050)}", # Simulating a pool of 50 users
        "amount": round(fake.pyfloat(left_digits=4, right_digits=2, positive=True, min_value=5.0, max_value=1500.0), 2),
        "merchant": fake.company(),
        "location": fake.city(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

print("Igniting the Velocity Engine... Press Ctrl+C to stop.")

try:
    while True:
        # 1. Generate the raw data
        transaction = generate_transaction()

        # 2. Serialize to JSON (Kafka only accepts bytes, not Python dictionaries)
        payload = json.dumps(transaction)

        # 3. Produce to Kafka
        # We use user_id as the 'key'. This ensures all transactions for the same user 
        # always go to the exact same Kafka partition (crucial for distributed ordering).
        producer.produce(
            topic=topic,
            key=transaction['user_id'].encode('utf-8'),
            value=payload.encode('utf-8'),
            callback=delivery_report
        )

        # 4. Trigger callbacks to clear the buffer
        producer.poll(0)

        # 5. Throttle the velocity (2 events per second for testing)
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\nShutting down producer gracefully...")
finally:
    # Data King Rule: Never leave messages stranded in memory.
    # flush() forces any remaining messages in the buffer to be sent before quitting.
    producer.flush()
    print("Producer closed cleanly.")