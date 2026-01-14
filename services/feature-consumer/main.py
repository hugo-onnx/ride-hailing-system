import json
import time
from confluent_kafka import Consumer, KafkaException
import redis

# Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "madrid-feature-service",
    "auto.offset.reset": "latest",
}

def get_redis_client():
    """Retry Redis connection"""
    while True:
        try:
            client = redis.Redis(host="redis", port=6379, decode_responses=True)
            client.ping()
            print("Successfully connected to Redis")
            return client
        except Exception as e:
            print(f"Waiting for Redis... {e}")
            time.sleep(3)

def get_consumer():
    """Retry Kafka connection"""
    while True:
        try:
            consumer = Consumer(KAFKA_CONFIG)
            # Test connection
            consumer.list_topics(timeout=5)
            print("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(3)

# Initialize with retry logic
redis_client = get_redis_client()
consumer = get_consumer()

TOPIC = "rides.requested.madrid"
consumer.subscribe([TOPIC])

print("Feature consumer started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        h3_index = event["h3_res8"]

        key = f"madrid:{h3_index}:raw_count"
        redis_client.incr(key)

        print(f"[CONSUMED] {event['event_id']} → {key}")

except KeyboardInterrupt:
    print("Aborted by user")
finally:
    consumer.close()