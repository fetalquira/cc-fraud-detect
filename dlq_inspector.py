import json
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'forensic_audit_team',
    'auto.offset.reset': 'earliest' # Start from the very beginning of the DLQ
}

consumer = Consumer(conf)
consumer.subscribe(['dlq_application_errors'])

print("🕵️ Forensic Inspector active. Reading Dead Letter Queue...\n")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Decode the Kafka message
        key = msg.key().decode('utf-8') if msg.key() else "NO_KEY"
        val = json.loads(msg.value().decode('utf-8'))
        
        print(f"🚨 QUARANTINED MESSAGE (Key: {key})")
        print(f"Reason: {val.get('failure_reason')}")
        print(f"Corrupt Payload: {json.dumps(val.get('original_payload'), indent=2)}")
        print("-" * 50)

except KeyboardInterrupt:
    print("\nClosing inspector.")
finally:
    consumer.close()