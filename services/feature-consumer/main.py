import json
import time
import redis
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from services.common.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, CITY
from services.common.time_utils import floor_timestamp, window_ttl_seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": f"{CITY}-feature-service",
    "auto.offset.reset": "latest",
}

# Topics
RIDE_TOPIC = f"rides.requested.{CITY}"
DRIVER_TOPIC = f"drivers.location.{CITY}"
TOPICS = [RIDE_TOPIC, DRIVER_TOPIC]

# Feature configuration
WINDOWS_MINUTES = [1, 5, 15]
AVG_IDLE_SPEED_KMH = 12


def get_redis_client():
    """Retry Redis connection"""
    while True:
        try:
            client = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True
            )
            client.ping()
            logger.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except Exception as e:
            logger.warning(f"Waiting for Redis... {e}")
            time.sleep(3)


def get_consumer():
    """Retry Kafka connection"""
    while True:
        try:
            consumer = Consumer(KAFKA_CONFIG)
            consumer.list_topics(timeout=5)
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)


def wait_for_topics(consumer: Consumer, topics: list[str], timeout: int = 120):
    """Wait for all topics to become available in Kafka"""
    start_time = time.time()
    pending_topics = set(topics)
    
    while pending_topics and (time.time() - start_time < timeout):
        try:
            metadata = consumer.list_topics(timeout=10)
            
            for topic in list(pending_topics):
                if topic in metadata.topics:
                    topic_metadata = metadata.topics[topic]
                    if topic_metadata.error is None:
                        logger.info(f"Topic {topic} is available with {len(topic_metadata.partitions)} partitions")
                        pending_topics.remove(topic)
                    else:
                        logger.warning(f"Topic {topic} has error: {topic_metadata.error}")
                        
        except Exception as e:
            logger.warning(f"Error checking topic metadata: {e}")
        
        if pending_topics:
            logger.info(f"Waiting for topics: {pending_topics}")
            time.sleep(3)
    
    if pending_topics:
        raise TimeoutError(f"Topics {pending_topics} not available after {timeout} seconds")
    
    logger.info("All topics are available")


def process_ride_event(event: dict, redis_client: redis.Redis) -> bool:
    """
    Process a ride request event and update Redis with tumbling window aggregations.
    
    Updates demand-side features:
    - ride_requests: count of ride requests in the window
    """
    try:
        h3_index = event.get("h3_res8")
        event_id = event.get("event_id", "unknown")
        timestamp_str = event.get("timestamp")
        
        if not h3_index:
            logger.warning(f"Ride event {event_id} missing h3_res8 field")
            return False
        
        if not timestamp_str:
            logger.warning(f"Ride event {event_id} missing timestamp field")
            return False
        
        # Parse timestamp
        ts = datetime.fromisoformat(timestamp_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        
        # Update each tumbling window
        for window in WINDOWS_MINUTES:
            window_start = floor_timestamp(ts, window)
            window_key = f"{CITY}:{h3_index}:{window}m:{window_start.isoformat()}"
            
            pipe = redis_client.pipeline()
            
            # Demand feature - increment ride requests
            pipe.hincrby(window_key, "ride_requests", 1)
            
            # Metadata (idempotent - only set if not exists)
            pipe.hsetnx(window_key, "window_minutes", window)
            pipe.hsetnx(window_key, "h3_res8", h3_index)
            pipe.hsetnx(window_key, "window_start", window_start.isoformat())
            
            # Set TTL for automatic cleanup
            pipe.expire(window_key, window_ttl_seconds(window))
            
            pipe.execute()
        
        logger.debug(f"[DEMAND] event={event_id} h3={h3_index}")
        return True
        
    except redis.RedisError as e:
        logger.error(f"Redis error processing ride event: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Invalid timestamp in ride event: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error processing ride event: {e}", exc_info=True)
        return False


def process_driver_event(event: dict, redis_client: redis.Redis) -> bool:
    """
    Process a driver location event and update Redis with tumbling window aggregations.
    
    Updates supply-side features:
    - available_drivers: count of available driver pings
    - active_drivers: count of on-trip driver pings
    - deadhead_km_sum: estimated deadhead kilometers (idle drivers)
    - idle_events: count of idle driver events
    """
    try:
        h3_index = event.get("h3_res8")
        driver_id = event.get("driver_id", "unknown")
        timestamp_str = event.get("timestamp")
        status = event.get("status")
        idle_seconds = event.get("idle_seconds", 0)
        
        if not h3_index:
            logger.warning(f"Driver event {driver_id} missing h3_res8 field")
            return False
        
        if not timestamp_str:
            logger.warning(f"Driver event {driver_id} missing timestamp field")
            return False
        
        # Parse timestamp
        ts = datetime.fromisoformat(timestamp_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        
        # Calculate deadhead kilometers for available drivers
        deadhead_km = (
            idle_seconds * AVG_IDLE_SPEED_KMH / 3600
            if status == "available"
            else 0
        )
        
        # Update each tumbling window
        for window in WINDOWS_MINUTES:
            window_start = floor_timestamp(ts, window)
            window_key = f"{CITY}:{h3_index}:{window}m:{window_start.isoformat()}"
            
            pipe = redis_client.pipeline()
            
            # Supply features based on driver status
            if status == "available":
                pipe.hincrby(window_key, "available_drivers", 1)
                pipe.hincrbyfloat(window_key, "deadhead_km_sum", deadhead_km)
                pipe.hincrby(window_key, "idle_events", 1)
            else:
                pipe.hincrby(window_key, "active_drivers", 1)
            
            # Metadata (idempotent)
            pipe.hsetnx(window_key, "window_minutes", window)
            pipe.hsetnx(window_key, "h3_res8", h3_index)
            pipe.hsetnx(window_key, "window_start", window_start.isoformat())
            
            # Set TTL for automatic cleanup
            pipe.expire(window_key, window_ttl_seconds(window))
            
            pipe.execute()
        
        logger.debug(f"[SUPPLY] driver={driver_id} h3={h3_index} status={status}")
        return True
        
    except redis.RedisError as e:
        logger.error(f"Redis error processing driver event: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Invalid timestamp in driver event: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error processing driver event: {e}", exc_info=True)
        return False


def main():
    """Main consumer loop - processes both ride and driver events"""
    redis_client = get_redis_client()
    consumer = get_consumer()
    
    # Wait for all topics to be created by producer
    wait_for_topics(consumer, TOPICS)
    
    consumer.subscribe(TOPICS)
    logger.info(f"Feature consumer started (tumbling windows enabled)")
    logger.info(f"  Subscribed to: {TOPICS}")
    logger.info(f"  Window sizes: {WINDOWS_MINUTES} minutes")
    
    ride_count = 0
    driver_count = 0
    last_log_time = time.time()
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Topic temporarily unavailable, waiting...")
                    time.sleep(5)
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                event = json.loads(msg.value().decode("utf-8"))
                topic = msg.topic()
                
                # Route to appropriate processor based on topic
                if topic == RIDE_TOPIC:
                    if process_ride_event(event, redis_client):
                        ride_count += 1
                elif topic == DRIVER_TOPIC:
                    if process_driver_event(event, redis_client):
                        driver_count += 1
                else:
                    logger.warning(f"Unknown topic: {topic}")
                
                # Log summary every 10 seconds
                if time.time() - last_log_time >= 10:
                    logger.info(
                        f"Processed {ride_count} ride events, "
                        f"{driver_count} driver events"
                    )
                    last_log_time = time.time()
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
            except Exception as e:
                logger.error(f"Error handling message: {e}", exc_info=True)
    
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        logger.info("Closing consumer...")
        consumer.close()
        redis_client.close()
        logger.info("Feature consumer stopped")


if __name__ == "__main__":
    main()