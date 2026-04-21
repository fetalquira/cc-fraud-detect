import json
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_detection_squad',  # The Consumer Group ID
    'auto.offset.reset': 'earliest',      # Where to start reading if no previous offset exists
    'enable.auto.commit': True            # Automatically save our place in the stream
}

# Initialize the Consumer
consumer = Consumer(conf)
topic = 'raw_transactions'

# Subscribe to the topic
consumer.subscribe([topic])

print("Consumer activated. Listening for transactions... Press Ctrl+C to stop.")

try:
    while True:
        # 1. The Pull Model: Ask Kafka for a message, wait up to 1.0 second.
        msg = consumer.poll(timeout=1.0)

        # 2. Handle empty responses (no new data right now)
        if msg is None:
            continue

        # 3. Handle Kafka Errors
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event (not a real error, just reached the end of current data)
                continue
            elif msg.error():
                raise KafkaException(msg.error())

        # 4. Process the Message
        # Kafka messages are bytes. We must decode them back to strings, then parse the JSON.
        try:
            raw_value = msg.value().decode('utf-8')
            transaction = json.loads(raw_value)
            
            print(f"Received Transaction: {transaction['user_id']} spent ${transaction['amount']} at {transaction['merchant']}")
            
        except json.JSONDecodeError:
            print(f"Corrupt data received: {msg.value()}")

except KeyboardInterrupt:
    print("\nShutting down consumer gracefully...")
finally:
    # Data King Rule: Always close the consumer cleanly. 
    # This tells Kafka "I am leaving" so it can reassign my partitions to another consumer if needed,
    # and it commits the final offsets to disk.
    consumer.close()
    print("Consumer closed.")