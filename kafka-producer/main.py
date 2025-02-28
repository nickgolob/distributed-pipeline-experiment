from kafka import KafkaProducer
import json
import time
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(10):
    message = {"id": i, "value": f"message-{i}"}
    producer.send("test-topic", message)
    print(f"Produced: {message}")
    time.sleep(1)

producer.close()
