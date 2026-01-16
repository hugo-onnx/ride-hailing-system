import h3
import json
import time
import uuid
import random
import logging

from datetime import datetime, timezone
from confluent_kafka import Producer

from services.common.config import KAFKA_BOOTSTRAP_SERVERS, CITY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
TOPIC = f"rides.requested.{CITY}"
MADRID_CENTER = (40.4168, -3.7038)
LAT_RANGE = 0.02  # ~2.2 km range
LNG_RANGE = 0.02
H3_RESOLUTION = 8
EVENT_INTERVAL = 1.0

def get_producer():
    """Retries connection until Kafka is available"""
    while True:
        try:
            p = Producer(KAFKA_CONFIG)
            p.poll(0)
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return p
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)

def delivery_report(err, msg):
    """Callback for Kafka message delivery reports"""
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Message delivered to partition {msg.partition()}")

def generate_ride_event():
    """Generate a random ride request event"""
    lat = MADRID_CENTER[0] + random.uniform(-LAT_RANGE, LAT_RANGE)
    lng = MADRID_CENTER[1] + random.uniform(-LNG_RANGE, LNG_RANGE)
    
    h3_index = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "h3_res8": h3_index,
    }
    
    return event

def main():
    """Main event producer loop"""
    producer = get_producer()
    logger.info(f"Starting event producer for topic: {TOPIC}")
    
    try:
        while True:
            try:
                event = generate_ride_event()
                
                producer.produce(
                    topic=TOPIC,
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )
                producer.poll(0)
                time.sleep(EVENT_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error producing event: {e}", exc_info=True)
                time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("Shutting down event producer...")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()
        logger.info("Event producer stopped")

if __name__ == "__main__":
    main()