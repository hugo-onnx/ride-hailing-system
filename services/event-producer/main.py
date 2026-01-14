import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer
import h3

KAFKA_BOOTSTRAP = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "rides.requested.madrid"

MADRID_CENTER = (40.4168, -3.7038)

def random_point_nearby(lat, lng, delta=0.02):
    return (
        lat + random.uniform(-delta, delta),
        lng + random.uniform(-delta, delta),
    )

while True:
    lat, lng = random_point_nearby(*MADRID_CENTER)
    h3_index = h3.geo_to_h3(lat, lng, 8)

    event = {
        "event_id": str(random.randint(1_000_000, 9_999_999)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pickup_lat": lat,
        "pickup_lng": lng,
        "h3_res8": h3_index,
        "estimated_distance_km": round(random.uniform(1, 10), 2),
    }

    producer.send(TOPIC, event)
    print(f"[PRODUCED] {event}")

    time.sleep(1)
