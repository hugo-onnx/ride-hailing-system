import h3
import json
import time
import uuid
import random
import logging
import math

from datetime import datetime, timezone
from dataclasses import dataclass
from confluent_kafka import Producer
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    start_http_server
)

from services.common.config import KAFKA_BOOTSTRAP_SERVERS, CITY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
RIDE_TOPIC = f"rides.requested.{CITY}"
DRIVER_TOPIC = f"drivers.location.{CITY}"
H3_RESOLUTION = 8
METRICS_PORT = 8001

RIDES_PRODUCED = Counter(
    'event_producer_rides_produced_total',
    'Total number of ride request events produced',
    ['city', 'zone']
)

DRIVER_PINGS_PRODUCED = Counter(
    'event_producer_driver_pings_produced_total',
    'Total number of driver location events produced',
    ['city', 'status']
)

KAFKA_ERRORS = Counter(
    'event_producer_kafka_errors_total',
    'Total number of Kafka delivery errors',
    ['city', 'topic']
)

ACTIVE_DRIVERS = Gauge(
    'event_producer_active_drivers',
    'Current number of simulated drivers',
    ['city', 'status']
)

BATCH_SIZE_GAUGE = Gauge(
    'event_producer_batch_size',
    'Current batch size configuration',
    ['city']
)

PRODUCE_LATENCY = Histogram(
    'event_producer_produce_latency_seconds',
    'Time taken to produce a batch of events',
    ['city'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

EVENTS_PER_SECOND = Summary(
    'event_producer_events_per_second',
    'Events produced per second',
    ['city', 'type']
)

EVENT_INTERVAL = 0.5
RIDES_PER_BATCH = 10
NUM_DRIVERS = 2000
DRIVER_PING_INTERVAL = 5
DRIVER_DRIFT_STRENGTH = 0.3
DRIVER_MOVE_DELTA = 0.002

# LOW VOLUME - Testing/Development
# EVENT_INTERVAL = 1.0
# RIDES_PER_BATCH = 1
# NUM_DRIVERS = 200
# DRIVER_PING_INTERVAL = 5

# MEDIUM VOLUME - Normal day
# EVENT_INTERVAL = 0.5
# RIDES_PER_BATCH = 5
# NUM_DRIVERS = 1000
# DRIVER_PING_INTERVAL = 5

# HIGH VOLUME - Rush hour (DEFAULT)
# EVENT_INTERVAL = 0.5
# RIDES_PER_BATCH = 10
# NUM_DRIVERS = 2000
# DRIVER_PING_INTERVAL = 5

# EXTREME VOLUME - Peak demand
# EVENT_INTERVAL = 0.25
# RIDES_PER_BATCH = 20
# NUM_DRIVERS = 3000
# DRIVER_PING_INTERVAL = 3


@dataclass
class Zone:
    """Represents a demand zone in the city"""
    name: str
    lat: float
    lng: float
    radius: float
    demand_weight: float
    driver_weight: float



MADRID_ZONES = [
    Zone("Sol-Gran Via", 40.4169, -3.7034, 0.8, demand_weight=20, driver_weight=25),
    Zone("Plaza Mayor", 40.4155, -3.7074, 0.5, demand_weight=12, driver_weight=15),
    Zone("Atocha", 40.4065, -3.6895, 0.6, demand_weight=15, driver_weight=18),
    Zone("Chamartin", 40.4722, -3.6824, 0.6, demand_weight=12, driver_weight=14),
    Zone("Principe Pio", 40.4210, -3.7205, 0.4, demand_weight=8, driver_weight=10),
    Zone("Barajas T1-T3", 40.4719, -3.5674, 0.8, demand_weight=14, driver_weight=12),
    Zone("Barajas T4", 40.4929, -3.5922, 0.5, demand_weight=10, driver_weight=8),
    Zone("Azca", 40.4505, -3.6925, 0.5, demand_weight=10, driver_weight=12),
    Zone("Cuatro Torres", 40.4748, -3.6875, 0.4, demand_weight=8, driver_weight=10),
    Zone("Mendez Alvaro", 40.3977, -3.6690, 0.5, demand_weight=7, driver_weight=8),
    Zone("Malasana", 40.4260, -3.7060, 0.4, demand_weight=9, driver_weight=10),
    Zone("Chueca", 40.4225, -3.6970, 0.4, demand_weight=9, driver_weight=10),
    Zone("La Latina", 40.4115, -3.7115, 0.4, demand_weight=8, driver_weight=9),
    Zone("Lavapies", 40.4085, -3.7015, 0.4, demand_weight=7, driver_weight=8),
    Zone("Salamanca", 40.4280, -3.6820, 0.6, demand_weight=8, driver_weight=10),
    Zone("Goya", 40.4235, -3.6755, 0.4, demand_weight=6, driver_weight=8),
    Zone("Moncloa", 40.4350, -3.7195, 0.6, demand_weight=5, driver_weight=6),
    Zone("Arguelles", 40.4305, -3.7145, 0.4, demand_weight=5, driver_weight=6),
    Zone("Tetuan", 40.4605, -3.6985, 0.5, demand_weight=4, driver_weight=5),
    Zone("Vallecas", 40.3785, -3.6515, 0.7, demand_weight=4, driver_weight=4),
    Zone("Carabanchel", 40.3850, -3.7350, 0.7, demand_weight=4, driver_weight=4),
    Zone("Usera", 40.3855, -3.7015, 0.5, demand_weight=3, driver_weight=3),
    Zone("Ciudad Universitaria", 40.4485, -3.7295, 0.5, demand_weight=5, driver_weight=5),
    Zone("Hospital La Paz", 40.4815, -3.6875, 0.3, demand_weight=4, driver_weight=4),
    Zone("Hospital 12 Octubre", 40.3755, -3.6975, 0.3, demand_weight=4, driver_weight=4),
    Zone("Santiago Bernabeu", 40.4531, -3.6883, 0.3, demand_weight=5, driver_weight=6),
    Zone("Wanda Metropolitano", 40.4362, -3.5995, 0.4, demand_weight=4, driver_weight=4),
    Zone("Retiro", 40.4153, -3.6845, 0.6, demand_weight=4, driver_weight=3),
    Zone("Casa de Campo", 40.4195, -3.7495, 0.8, demand_weight=2, driver_weight=2),
]


def normalize_weights(zones: list[Zone], attr: str) -> list[float]:
    weights = [getattr(z, attr) for z in zones]
    total = sum(weights)
    return [w / total for w in weights]


def sample_point_in_zone(zone: Zone) -> tuple[float, float]:
    sigma_lat = zone.radius / 111.0 / 2
    sigma_lng = zone.radius / (111.0 * math.cos(math.radians(zone.lat))) / 2
    lat = random.gauss(zone.lat, sigma_lat)
    lng = random.gauss(zone.lng, sigma_lng)
    return lat, lng


def generate_ride_location() -> tuple[float, float, str]:
    demand_weights = normalize_weights(MADRID_ZONES, "demand_weight")
    zone = random.choices(MADRID_ZONES, weights=demand_weights)[0]
    lat, lng = sample_point_in_zone(zone)
    return lat, lng, zone.name


class DriverSimulator:
    def __init__(self, num_drivers: int, ping_interval: float, event_interval: float):
        self.drivers = {}
        self.ping_interval = ping_interval
        self.event_interval = event_interval
        self.tick_count = 0
        self.pings_per_interval = max(1, int(ping_interval / event_interval))
        self._demand_weights = normalize_weights(MADRID_ZONES, "demand_weight")
        self._init_drivers(num_drivers)
    
    def _init_drivers(self, num_drivers: int):
        driver_weights = normalize_weights(MADRID_ZONES, "driver_weight")
        
        for i in range(num_drivers):
            driver_id = f"d_{i:05d}"
            zone = random.choices(MADRID_ZONES, weights=driver_weights)[0]
            lat, lng = sample_point_in_zone(zone)
            ping_offset = i % self.pings_per_interval
            
            self.drivers[driver_id] = {
                "lat": lat,
                "lng": lng,
                "status": "available",
                "home_zone": zone.name,
                "ticks_in_status": 0,
                "ping_offset": ping_offset,
            }
        
        logger.info(f"Initialized {num_drivers} drivers across {len(MADRID_ZONES)} zones")
    
    def _get_nearest_demand_zone(self, lat: float, lng: float) -> Zone:
        best_zone = None
        best_score = -1
        
        for zone, weight in zip(MADRID_ZONES, self._demand_weights):
            dist = math.sqrt((lat - zone.lat)**2 + (lng - zone.lng)**2)
            dist = max(dist, 0.001)
            score = weight / dist
            if score > best_score:
                best_score = score
                best_zone = zone
        
        return best_zone
    
    def _move_driver(self, driver_id: str, state: dict) -> tuple[float, float]:
        lat, lng = state["lat"], state["lng"]
        
        if state["status"] == "available":
            target_zone = self._get_nearest_demand_zone(lat, lng)
            dlat = target_zone.lat - lat
            dlng = target_zone.lng - lng
            dist = math.sqrt(dlat**2 + dlng**2)
            
            if dist > 0.001:
                dlat /= dist
                dlng /= dist
                random_lat = random.uniform(-1, 1)
                random_lng = random.uniform(-1, 1)
                move_lat = DRIVER_DRIFT_STRENGTH * dlat + (1 - DRIVER_DRIFT_STRENGTH) * random_lat
                move_lng = DRIVER_DRIFT_STRENGTH * dlng + (1 - DRIVER_DRIFT_STRENGTH) * random_lng
                move_dist = math.sqrt(move_lat**2 + move_lng**2)
                if move_dist > 0:
                    lat += (move_lat / move_dist) * DRIVER_MOVE_DELTA
                    lng += (move_lng / move_dist) * DRIVER_MOVE_DELTA
        else:
            lat += random.uniform(-DRIVER_MOVE_DELTA * 2, DRIVER_MOVE_DELTA * 2)
            lng += random.uniform(-DRIVER_MOVE_DELTA * 2, DRIVER_MOVE_DELTA * 2)
        
        return lat, lng
    
    def _update_status(self, state: dict) -> str:
        state["ticks_in_status"] += 1
        
        if state["status"] == "available":
            trip_chance = min(0.02 + state["ticks_in_status"] * 0.005, 0.15)
            if random.random() < trip_chance:
                state["status"] = "on_trip"
                state["ticks_in_status"] = 0
        else:
            complete_chance = state["ticks_in_status"] / 50.0
            if random.random() < complete_chance:
                state["status"] = "available"
                state["ticks_in_status"] = 0
        
        return state["status"]
    
    def generate_events(self) -> list[dict]:
        events = []
        current_offset = self.tick_count % self.pings_per_interval
        
        for driver_id, state in self.drivers.items():
            if state["ping_offset"] != current_offset:
                continue
            
            lat, lng = self._move_driver(driver_id, state)
            state["lat"], state["lng"] = lat, lng
            status = self._update_status(state)
            
            idle_seconds = (
                state["ticks_in_status"] * self.ping_interval 
                if status == "available" else 0
            )
            
            event = {
                "driver_id": driver_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "lat": lat,
                "lng": lng,
                "h3_res8": h3.latlng_to_cell(lat, lng, H3_RESOLUTION),
                "status": status,
                "idle_seconds": int(idle_seconds),
            }
            events.append(event)
        
        self.tick_count += 1
        return events
    
    def get_stats(self) -> dict:
        available = sum(1 for d in self.drivers.values() if d["status"] == "available")
        on_trip = len(self.drivers) - available
        return {
            "total": len(self.drivers),
            "available": available,
            "on_trip": on_trip,
            "availability_rate": available / len(self.drivers) * 100,
        }


def get_producer():
    while True:
        try:
            p = Producer(KAFKA_CONFIG)
            p.poll(0)
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return p
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
        KAFKA_ERRORS.labels(city=CITY, topic=msg.topic()).inc()
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def generate_ride_event() -> dict:
    lat, lng, zone = generate_ride_location()
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat": lat,
        "lng": lng,
        "h3_res8": h3.latlng_to_cell(lat, lng, H3_RESOLUTION),
        "zone": zone,
    }


def update_driver_metrics(driver_sim: DriverSimulator):
    """Update Prometheus gauges for driver status"""
    stats = driver_sim.get_stats()
    ACTIVE_DRIVERS.labels(city=CITY, status="available").set(stats["available"])
    ACTIVE_DRIVERS.labels(city=CITY, status="on_trip").set(stats["on_trip"])


def main():
    start_http_server(METRICS_PORT)
    logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
    
    producer = get_producer()
    driver_sim = DriverSimulator(NUM_DRIVERS, DRIVER_PING_INTERVAL, EVENT_INTERVAL)
    
    BATCH_SIZE_GAUGE.labels(city=CITY).set(RIDES_PER_BATCH)
    
    rides_per_min = RIDES_PER_BATCH / EVENT_INTERVAL * 60
    driver_pings_per_min = NUM_DRIVERS / DRIVER_PING_INTERVAL * 60
    
    logger.info("=" * 60)
    logger.info("Event Producer with Prometheus Metrics")
    logger.info("=" * 60)
    logger.info(f"  Metrics endpoint: http://localhost:{METRICS_PORT}/metrics")
    logger.info(f"  Ride topic: {RIDE_TOPIC}")
    logger.info(f"  Driver topic: {DRIVER_TOPIC}")
    logger.info(f"  Expected: {rides_per_min:.0f} rides/min, {driver_pings_per_min:.0f} pings/min")
    logger.info("=" * 60)
    
    batch_count = 0
    total_rides = 0
    total_driver_pings = 0
    start_time = time.time()
    last_log_time = start_time
    last_metric_time = start_time
    
    try:
        while True:
            batch_start = time.time()
            
            for _ in range(RIDES_PER_BATCH):
                ride_event = generate_ride_event()
                producer.produce(
                    topic=RIDE_TOPIC,
                    value=json.dumps(ride_event).encode("utf-8"),
                    callback=delivery_report
                )
                total_rides += 1
                RIDES_PRODUCED.labels(city=CITY, zone=ride_event["zone"]).inc()
            
            driver_events = driver_sim.generate_events()
            for event in driver_events:
                producer.produce(
                    topic=DRIVER_TOPIC,
                    key=event["driver_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )
                total_driver_pings += 1
                DRIVER_PINGS_PRODUCED.labels(city=CITY, status=event["status"]).inc()
            
            producer.poll(0)
            batch_count += 1
            
            batch_duration = time.time() - batch_start
            PRODUCE_LATENCY.labels(city=CITY).observe(batch_duration)
            
            if time.time() - last_metric_time >= 1.0:
                update_driver_metrics(driver_sim)
                last_metric_time = time.time()
            
            if time.time() - last_log_time >= 10:
                elapsed = time.time() - start_time
                stats = driver_sim.get_stats()
                
                EVENTS_PER_SECOND.labels(city=CITY, type="rides").observe(total_rides / elapsed)
                EVENTS_PER_SECOND.labels(city=CITY, type="driver_pings").observe(total_driver_pings / elapsed)
                
                logger.info(
                    f"[{elapsed:.0f}s] "
                    f"Rides: {total_rides} ({total_rides/elapsed:.1f}/s) | "
                    f"Pings: {total_driver_pings} ({total_driver_pings/elapsed:.1f}/s) | "
                    f"Drivers: {stats['available']}/{stats['total']} available"
                )
                last_log_time = time.time()
            
            sleep_time = max(0, EVENT_INTERVAL - batch_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.flush()
        logger.info("Event producer stopped")


if __name__ == "__main__":
    main()