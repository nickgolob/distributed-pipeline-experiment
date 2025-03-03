from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MAX_RETRIES = 10  # Number of times to retry connection
RETRY_DELAY = 1   # Seconds to wait before retrying

print("🚀 Kafka Producer started!")

def create_producer():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            retries += 1
            print(f"Kafka broker not available. Retrying {retries}/{MAX_RETRIES}...")
            time.sleep(RETRY_DELAY)
    raise Exception("Failed to connect to Kafka broker after multiple retries.")

producer = create_producer()

for i in range(1000):
    message = {"id": i, "value": f"message-{i}"}
    producer.send("test-topic", message)
    print(f"Produced: {message}")
    time.sleep(1)

producer.close()