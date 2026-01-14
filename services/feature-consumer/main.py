import json
from kafka import KafkaConsumer
import redis

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "rides.requested.madrid"

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

print("Feature consumer started...")

for message in consumer:
    event = message.value
    h3_index = event["h3_res8"]

    # For now: just count requests per hex
    key = f"madrid:{h3_index}:raw_count"
    redis_client.incr(key)

    print(f"[CONSUMED] {event['event_id']} → {key}")
