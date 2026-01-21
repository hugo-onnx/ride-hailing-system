import json
import time
import redis
import logging
from datetime import datetime, timezone
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    start_http_server
)

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
    "fetch.min.bytes": 1024,
    "fetch.wait.max.ms": 100,
    "session.timeout.ms": 30000,
    "heartbeat.interval.ms": 10000,
    "queued.min.messages": 10000,
    "queued.max.messages.kbytes": 65536,
}

# Topics
RIDE_TOPIC = f"rides.requested.{CITY}"
DRIVER_TOPIC = f"drivers.location.{CITY}"
TOPICS = [RIDE_TOPIC, DRIVER_TOPIC]

# Feature configuration
WINDOWS_MINUTES = [1, 5, 15]
AVG_IDLE_SPEED_KMH = 12

# Batching configuration
BATCH_SIZE = 100
BATCH_TIMEOUT_MS = 200

# Metrics port
METRICS_PORT = 8002

# =============================================================================
# PROMETHEUS METRICS
# =============================================================================

# Counters
EVENTS_CONSUMED = Counter(
    'feature_consumer_events_consumed_total',
    'Total events consumed from Kafka',
    ['city', 'topic', 'status']
)

EVENTS_PROCESSED = Counter(
    'feature_consumer_events_processed_total',
    'Total events successfully processed',
    ['city', 'type']
)

PROCESSING_ERRORS = Counter(
    'feature_consumer_processing_errors_total',
    'Total processing errors',
    ['city', 'type', 'error_type']
)

REDIS_OPERATIONS = Counter(
    'feature_consumer_redis_operations_total',
    'Total Redis operations',
    ['city', 'operation']
)

FLUSHES = Counter(
    'feature_consumer_flushes_total',
    'Total batch flushes to Redis',
    ['city']
)

# Gauges
BATCH_PENDING_EVENTS = Gauge(
    'feature_consumer_batch_pending_events',
    'Number of events pending in batch',
    ['city']
)

BATCH_PENDING_KEYS = Gauge(
    'feature_consumer_batch_pending_keys',
    'Number of Redis keys pending in batch',
    ['city']
)

CONSUMER_LAG = Gauge(
    'feature_consumer_lag_seconds',
    'Estimated consumer lag in seconds',
    ['city']
)

# Histograms
FLUSH_DURATION = Histogram(
    'feature_consumer_flush_duration_seconds',
    'Time taken to flush batch to Redis',
    ['city'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

EVENT_PROCESSING_TIME = Histogram(
    'feature_consumer_event_processing_seconds',
    'Time to process a single event',
    ['city', 'type'],
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05]
)

# Summary
EVENTS_PER_FLUSH = Summary(
    'feature_consumer_events_per_flush',
    'Number of events per flush',
    ['city']
)

KEYS_PER_FLUSH = Summary(
    'feature_consumer_keys_per_flush',
    'Number of Redis keys updated per flush',
    ['city']
)


def get_redis_client():
    """Retry Redis connection with connection pooling"""
    while True:
        try:
            pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                max_connections=10,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            client = redis.Redis(connection_pool=pool)
            client.ping()
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
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
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)


def wait_for_topics(consumer: Consumer, topics: list[str], timeout: int = 120):
    """Wait for all topics to become available"""
    start_time = time.time()
    pending_topics = set(topics)
    
    while pending_topics and (time.time() - start_time < timeout):
        try:
            metadata = consumer.list_topics(timeout=10)
            for topic in list(pending_topics):
                if topic in metadata.topics:
                    topic_metadata = metadata.topics[topic]
                    if topic_metadata.error is None:
                        logger.info(f"Topic {topic} available ({len(topic_metadata.partitions)} partitions)")
                        pending_topics.remove(topic)
        except Exception as e:
            logger.warning(f"Error checking topics: {e}")
        
        if pending_topics:
            logger.info(f"Waiting for topics: {pending_topics}")
            time.sleep(3)
    
    if pending_topics:
        raise TimeoutError(f"Topics {pending_topics} not available after {timeout}s")


class FeatureAggregator:
    """Batches feature updates and flushes to Redis efficiently"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.pending_updates: dict[str, dict] = defaultdict(lambda: {
            "ride_requests": 0,
            "available_drivers": 0,
            "active_drivers": 0,
            "deadhead_km_sum": 0.0,
            "idle_events": 0,
            "metadata": None,
        })
        self.last_flush = time.time()
        self.events_since_flush = 0
    
    def add_ride_event(self, event: dict) -> bool:
        """Add a ride event to the batch"""
        start_time = time.time()
        try:
            h3_index = event.get("h3_res8")
            timestamp_str = event.get("timestamp")
            
            if not h3_index or not timestamp_str:
                PROCESSING_ERRORS.labels(city=CITY, type="ride", error_type="missing_field").inc()
                return False
            
            ts = datetime.fromisoformat(timestamp_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            
            # Calculate lag
            lag = (datetime.now(timezone.utc) - ts).total_seconds()
            CONSUMER_LAG.labels(city=CITY).set(lag)
            
            for window in WINDOWS_MINUTES:
                window_start = floor_timestamp(ts, window)
                window_key = f"{CITY}:{h3_index}:{window}m:{window_start.isoformat()}"
                
                self.pending_updates[window_key]["ride_requests"] += 1
                
                if self.pending_updates[window_key]["metadata"] is None:
                    self.pending_updates[window_key]["metadata"] = {
                        "window_minutes": window,
                        "h3_res8": h3_index,
                        "window_start": window_start.isoformat(),
                        "ttl": window_ttl_seconds(window),
                    }
            
            self.events_since_flush += 1
            EVENT_PROCESSING_TIME.labels(city=CITY, type="ride").observe(time.time() - start_time)
            return True
            
        except Exception as e:
            PROCESSING_ERRORS.labels(city=CITY, type="ride", error_type="exception").inc()
            logger.error(f"Error adding ride event: {e}")
            return False
    
    def add_driver_event(self, event: dict) -> bool:
        """Add a driver event to the batch"""
        start_time = time.time()
        try:
            h3_index = event.get("h3_res8")
            timestamp_str = event.get("timestamp")
            status = event.get("status")
            idle_seconds = event.get("idle_seconds", 0)
            
            if not h3_index or not timestamp_str:
                PROCESSING_ERRORS.labels(city=CITY, type="driver", error_type="missing_field").inc()
                return False
            
            ts = datetime.fromisoformat(timestamp_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            
            deadhead_km = (
                idle_seconds * AVG_IDLE_SPEED_KMH / 3600
                if status == "available"
                else 0
            )
            
            for window in WINDOWS_MINUTES:
                window_start = floor_timestamp(ts, window)
                window_key = f"{CITY}:{h3_index}:{window}m:{window_start.isoformat()}"
                
                if status == "available":
                    self.pending_updates[window_key]["available_drivers"] += 1
                    self.pending_updates[window_key]["deadhead_km_sum"] += deadhead_km
                    self.pending_updates[window_key]["idle_events"] += 1
                else:
                    self.pending_updates[window_key]["active_drivers"] += 1
                
                if self.pending_updates[window_key]["metadata"] is None:
                    self.pending_updates[window_key]["metadata"] = {
                        "window_minutes": window,
                        "h3_res8": h3_index,
                        "window_start": window_start.isoformat(),
                        "ttl": window_ttl_seconds(window),
                    }
            
            self.events_since_flush += 1
            EVENT_PROCESSING_TIME.labels(city=CITY, type="driver").observe(time.time() - start_time)
            return True
            
        except Exception as e:
            PROCESSING_ERRORS.labels(city=CITY, type="driver", error_type="exception").inc()
            logger.error(f"Error adding driver event: {e}")
            return False
    
    def should_flush(self) -> bool:
        """Check if we should flush the batch"""
        if self.events_since_flush >= BATCH_SIZE:
            return True
        if (time.time() - self.last_flush) * 1000 >= BATCH_TIMEOUT_MS:
            return True
        return False
    
    def flush(self) -> int:
        """Flush all pending updates to Redis"""
        if not self.pending_updates:
            return 0
        
        flush_start = time.time()
        
        try:
            pipe = self.redis.pipeline()
            keys_updated = 0
            
            for window_key, updates in self.pending_updates.items():
                metadata = updates["metadata"]
                if metadata is None:
                    continue
                
                if updates["ride_requests"] > 0:
                    pipe.hincrby(window_key, "ride_requests", updates["ride_requests"])
                
                if updates["available_drivers"] > 0:
                    pipe.hincrby(window_key, "available_drivers", updates["available_drivers"])
                
                if updates["active_drivers"] > 0:
                    pipe.hincrby(window_key, "active_drivers", updates["active_drivers"])
                
                if updates["deadhead_km_sum"] > 0:
                    pipe.hincrbyfloat(window_key, "deadhead_km_sum", updates["deadhead_km_sum"])
                
                if updates["idle_events"] > 0:
                    pipe.hincrby(window_key, "idle_events", updates["idle_events"])
                
                pipe.hsetnx(window_key, "window_minutes", metadata["window_minutes"])
                pipe.hsetnx(window_key, "h3_res8", metadata["h3_res8"])
                pipe.hsetnx(window_key, "window_start", metadata["window_start"])
                pipe.expire(window_key, metadata["ttl"])
                
                keys_updated += 1
            
            pipe.execute()
            
            # Record metrics
            flush_duration = time.time() - flush_start
            FLUSH_DURATION.labels(city=CITY).observe(flush_duration)
            FLUSHES.labels(city=CITY).inc()
            EVENTS_PER_FLUSH.labels(city=CITY).observe(self.events_since_flush)
            KEYS_PER_FLUSH.labels(city=CITY).observe(keys_updated)
            REDIS_OPERATIONS.labels(city=CITY, operation="pipeline_execute").inc()
            
            # Reset state
            flushed_events = self.events_since_flush
            self.pending_updates.clear()
            self.events_since_flush = 0
            self.last_flush = time.time()
            
            # Update gauges
            BATCH_PENDING_EVENTS.labels(city=CITY).set(0)
            BATCH_PENDING_KEYS.labels(city=CITY).set(0)
            
            return flushed_events
            
        except redis.RedisError as e:
            PROCESSING_ERRORS.labels(city=CITY, type="flush", error_type="redis_error").inc()
            logger.error(f"Redis error during flush: {e}")
            return 0
    
    def update_pending_metrics(self):
        """Update metrics for pending batch state"""
        BATCH_PENDING_EVENTS.labels(city=CITY).set(self.events_since_flush)
        BATCH_PENDING_KEYS.labels(city=CITY).set(len(self.pending_updates))


def main():
    """Main consumer loop with batched processing and metrics"""
    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
    
    redis_client = get_redis_client()
    consumer = get_consumer()
    
    wait_for_topics(consumer, TOPICS)
    consumer.subscribe(TOPICS)
    
    logger.info("=" * 60)
    logger.info("Feature Consumer with Prometheus Metrics")
    logger.info("=" * 60)
    logger.info(f"  Metrics endpoint: http://localhost:{METRICS_PORT}/metrics")
    logger.info(f"  Subscribed to: {TOPICS}")
    logger.info(f"  Window sizes: {WINDOWS_MINUTES} minutes")
    logger.info(f"  Batch size: {BATCH_SIZE}, timeout: {BATCH_TIMEOUT_MS}ms")
    logger.info("=" * 60)
    
    aggregator = FeatureAggregator(redis_client)
    
    ride_count = 0
    driver_count = 0
    flush_count = 0
    start_time = time.time()
    last_log_time = start_time
    
    try:
        while True:
            msg = consumer.poll(0.1)
            
            if msg is None:
                if aggregator.should_flush():
                    aggregator.flush()
                    flush_count += 1
                aggregator.update_pending_metrics()
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning("Topic unavailable, waiting...")
                    time.sleep(5)
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    PROCESSING_ERRORS.labels(city=CITY, type="kafka", error_type="consumer_error").inc()
                continue
            
            try:
                event = json.loads(msg.value().decode("utf-8"))
                topic = msg.topic()
                
                EVENTS_CONSUMED.labels(city=CITY, topic=topic, status="received").inc()
                
                if topic == RIDE_TOPIC:
                    if aggregator.add_ride_event(event):
                        ride_count += 1
                        EVENTS_PROCESSED.labels(city=CITY, type="ride").inc()
                elif topic == DRIVER_TOPIC:
                    if aggregator.add_driver_event(event):
                        driver_count += 1
                        EVENTS_PROCESSED.labels(city=CITY, type="driver").inc()
                
                if aggregator.should_flush():
                    aggregator.flush()
                    flush_count += 1
                
                aggregator.update_pending_metrics()
                
                # Log every 10 seconds
                if time.time() - last_log_time >= 10:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"[{elapsed:.0f}s] "
                        f"Rides: {ride_count} ({ride_count/elapsed:.1f}/s) | "
                        f"Drivers: {driver_count} ({driver_count/elapsed:.1f}/s) | "
                        f"Flushes: {flush_count}"
                    )
                    last_log_time = time.time()
                    
            except json.JSONDecodeError as e:
                PROCESSING_ERRORS.labels(city=CITY, type="parse", error_type="json_decode").inc()
                logger.error(f"Invalid JSON: {e}")
            except Exception as e:
                PROCESSING_ERRORS.labels(city=CITY, type="unknown", error_type="exception").inc()
                logger.error(f"Error: {e}")
    
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        aggregator.flush()
        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"Final: {ride_count} rides, {driver_count} drivers in {elapsed:.1f}s")
        logger.info("=" * 60)
        consumer.close()
        redis_client.close()


if __name__ == "__main__":
    main()