import h3
import time
import json
import redis
import logging
import numpy as np

from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from services.common.config import REDIS_HOST, REDIS_PORT, CITY
from features import fetch_window, WINDOWS
from derive import derive_features
from pricing import compute_price_multiplier
from monitoring.metrics import record_latency
from monitoring.drift import record_feature_snapshot
from eta.pickup_model import PickupETAEstimator
from eta.dropoff_model import DropoffETAEstimator
from eta.features import assemble_pickup_features, assemble_dropoff_features
from geo.utils import haversine_km

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

redis_client: redis.Redis | None = None
pickup_model: PickupETAEstimator | None = None
dropoff_model: DropoffETAEstimator | None = None

PICKUP_MODEL_PATH = "/app/models/pickup_eta_model.joblib"
DROPOFF_MODEL_PATH = "/app/models/dropoff_eta_model.joblib"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage Redis connection lifecycle."""
    global redis_client, pickup_model, dropoff_model
    
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True,
    )
    
    try:
        redis_client.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    try:
        pickup_model = PickupETAEstimator(PICKUP_MODEL_PATH)
        logger.info(f"Loaded ETA model from {PICKUP_MODEL_PATH}")
    except FileNotFoundError:
        logger.warning(f"ETA model not found at {PICKUP_MODEL_PATH}, ETA endpoint will be disabled")
        pickup_model = None
    except Exception as e:
        logger.error(f"Failed to load ETA model: {e}")
        pickup_model = None

    try:
        dropoff_model = DropoffETAEstimator(DROPOFF_MODEL_PATH)
        logger.info(f"Loaded Dropoff model from {DROPOFF_MODEL_PATH}")
    except FileNotFoundError:
        logger.warning(f"Dropoff model not found at {DROPOFF_MODEL_PATH}, trip quote endpoint will be disabled")
        dropoff_model = None
    except Exception as e:
        logger.error(f"Failed to load Dropoff model: {e}")
        dropoff_model = None

    yield
    
    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed")


app = FastAPI(
    title="Ride-Hailing Dynamic Pricing ETA System",
    description="Real-time feature retrieval for dynamic pricing inference",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    """Health check endpoint."""
    try:
        redis_client.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unhealthy: {e}")


@app.get("/v1/features")
def get_features(
    lat: float,
    lng: float,
    timestamp: str | None = None,
):
    """
    Get derived features for a location.
    
    Args:
        lat: Latitude
        lng: Longitude  
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        Feature vector with 1m, 5m, 15m window aggregations
    """
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    try:
        h3_index = h3.latlng_to_cell(lat, lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")

    feature_vector = {}
    for window in WINDOWS:
        raw = fetch_window(
            redis_client=redis_client,
            city=CITY,
            h3_index=h3_index,
            window=window,
            ts=ts,
        )
        derived = derive_features(raw)
        feature_vector[f"{window}m"] = derived

    return {
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": feature_vector,
    }


@app.get("/v1/features/debug")
def get_features_debug(
    lat: float,
    lng: float,
    timestamp: str | None = None,
):
    """
    Debug endpoint to inspect assembled feature vector with raw data.
    
    Same as /v1/features but includes raw Redis snapshots.
    """
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    try:
        h3_index = h3.latlng_to_cell(lat, lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")

    feature_vector = {}
    raw_snapshots = {}
    
    for window in WINDOWS:
        raw = fetch_window(
            redis_client=redis_client,
            city=CITY,
            h3_index=h3_index,
            window=window,
            ts=ts,
        )
        derived = derive_features(raw)
        feature_vector[f"{window}m"] = derived
        raw_snapshots[f"{window}m"] = raw

    return {
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": feature_vector,
        "raw": raw_snapshots,
    }


@app.post("/v1/pricing/quote")
def pricing_quote(
    lat: float,
    lng: float,
    timestamp: str | None = None,
):
    """
    Get a dynamic pricing quote for a location.
    
    Uses 5-minute window features to compute surge pricing multiplier.
    Includes monitoring for latency and feature drift.
    
    Args:
        lat: Latitude
        lng: Longitude
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        Pricing quote with multiplier and feature breakdown
    """
    start = time.perf_counter()

    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    try:
        h3_index = h3.latlng_to_cell(lat, lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")

    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_index,
        window=5,
        ts=ts,
    )

    features_5m = derive_features(raw_5m)
    pricing = compute_price_multiplier(features_5m)

    record_feature_snapshot(
        redis_client=redis_client,
        city=CITY,
        features=features_5m,
    )

    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "pricing", latency_ms)

    return {
        "city": CITY,
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": features_5m,
        "pricing": pricing,
        "latency_ms": round(latency_ms, 2),
    }


@app.get("/v1/monitoring/drift")
def drift_summary():
    """
    Get feature drift summary statistics.
    
    Analyzes stored feature snapshots to compute percentile statistics
    for key features, useful for detecting distribution drift over time.
    
    Returns:
        Summary with p50 and p95 for key features, or insufficient_data status
    """
    key = f"drift:{CITY}"
    data = redis_client.lrange(key, 0, -1)

    if len(data) < 50:
        return {"status": "insufficient_data", "samples": len(data)}

    parsed = [json.loads(x)["features"] for x in data]

    def summarize(field):
        values = [f[field] for f in parsed if field in f]
        if not values:
            return {"p50": 0.0, "p95": 0.0}
        return {
            "p50": round(float(np.percentile(values, 50)), 3),
            "p95": round(float(np.percentile(values, 95)), 3),
        }

    return {
        "city": CITY,
        "samples": len(parsed),
        "features": {
            "supply_demand_ratio": summarize("supply_demand_ratio"),
            "deadhead_km_avg": summarize("deadhead_km_avg"),
            "surge_pressure": summarize("surge_pressure"),
        },
    }


@app.post("/v1/eta/quote")
def eta_quote(
    lat: float,
    lng: float,
    trip_distance_km: float,
    timestamp: str | None = None,
):
    """
    Get an ETA estimate for a trip.
    
    Uses marketplace features and trip distance to predict
    estimated time of arrival using an XGBoost model.
    
    Args:
        lat: Pickup latitude
        lng: Pickup longitude
        trip_distance_km: Estimated trip distance in kilometers
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        ETA prediction in seconds and minutes with latency
    """
    if pickup_model is None:
        raise HTTPException(
            status_code=503, 
            detail="ETA model not loaded. Please ensure model file exists."
        )
    
    start = time.perf_counter()

    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    try:
        h3_index = h3.latlng_to_cell(lat, lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")

    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_index,
        window=5,
        ts=ts,
    )

    features_5m = derive_features(raw_5m)

    eta_features = assemble_pickup_features(
        trip_distance_km=trip_distance_km,
        features_5m=features_5m,
    )

    eta_seconds = pickup_model.predict(eta_features)

    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "eta", latency_ms)

    return {
        "city": CITY,
        "h3_res8": h3_index,
        "trip_distance_km": trip_distance_km,
        "eta_seconds": int(eta_seconds),
        "eta_minutes": round(eta_seconds / 60, 1),
        "latency_ms": round(latency_ms, 2),
    }


@app.post("/v1/quote")
def trip_quote(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    timestamp: str | None = None,
):
    """
    Get a complete trip quote with ETA and pricing.
    
    Combines pickup ETA, dropoff ETA, and dynamic pricing for a full
    trip estimate from origin to destination.
    
    Args:
        origin_lat, origin_lng: Pickup location coordinates
        dest_lat, dest_lng: Destination coordinates
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        Complete trip quote with ETAs and pricing
    """
    if pickup_model is None or dropoff_model is None:
        raise HTTPException(
            status_code=503, 
            detail="ETA models not loaded. Please ensure model files exist."
        )
    
    start = time.perf_counter()

    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    # --- GEO ---
    try:
        h3_origin = h3.latlng_to_cell(origin_lat, origin_lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid origin coordinates: {e}")
    
    trip_distance_km = haversine_km(
        origin_lat, origin_lng, dest_lat, dest_lng
    )
    
    if trip_distance_km < 0.1:
        raise HTTPException(status_code=400, detail="Origin and destination too close")

    # --- FEATURES ---
    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_origin,
        window=5,
        ts=ts,
    )
    features_5m = derive_features(raw_5m)

    # --- PICKUP ETA ---
    pickup_features = assemble_pickup_features(
        trip_distance_km=trip_distance_km,
        features_5m=features_5m,
    )
    pickup_eta = pickup_model.predict(pickup_features)

    # --- DROPOFF ETA ---
    dropoff_features = assemble_dropoff_features(
        trip_distance_km=trip_distance_km,
        features_5m=features_5m,
        ts=ts,
    )
    dropoff_eta = dropoff_model.predict(dropoff_features)

    # --- TOTAL ETA ---
    total_eta = pickup_eta + dropoff_eta

    # --- PRICING ---
    pricing = compute_price_multiplier(features_5m)
    base_fare = 1.2  # EUR
    price_per_km = 1.1  # EUR/km
    price = (base_fare + trip_distance_km * price_per_km) * pricing["multiplier"]

    # --- MONITORING ---
    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "trip_quote", latency_ms)

    return {
        "city": CITY,
        "h3_origin": h3_origin,
        "trip_distance_km": round(trip_distance_km, 2),
        "eta": {
            "pickup_seconds": int(pickup_eta),
            "dropoff_seconds": int(dropoff_eta),
            "total_seconds": int(total_eta),
            "total_minutes": round(total_eta / 60, 1),
        },
        "price": {
            "amount_eur": round(price, 2),
            "multiplier": pricing["multiplier"],
            "surge_level": pricing["surge_level"],
            "reasons": pricing["reasons"],
        },
        "latency_ms": round(latency_ms, 2),
    }