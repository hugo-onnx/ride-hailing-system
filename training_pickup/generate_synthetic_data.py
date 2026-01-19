import argparse
import numpy as np
import pandas as pd

from pathlib import Path


def generate_synthetic_trips(n_samples: int = 10000, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic trip data for pickup ETA prediction.
    
    Args:
        n_samples: Number of trip records to generate
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with trip records and features
    """
    np.random.seed(seed)
    
    start_date = pd.Timestamp("2026-01-01")
    timestamps = pd.date_range(
        start=start_date, 
        periods=n_samples, 
        freq="2min"
    )
    
    # Trip features
    trip_distance_km = np.random.lognormal(mean=1.5, sigma=0.7, size=n_samples)
    trip_distance_km = np.clip(trip_distance_km, 0.5, 50)
    
    # Marketplace features (simulating 5-minute window aggregations)
    # Add time-of-day patterns
    hour = timestamps.hour
    
    # Supply/demand ratio varies by time of day
    base_ratio = 5 + 3 * np.sin(2 * np.pi * hour / 24 - np.pi/2)
    supply_demand_ratio = np.abs(base_ratio + np.random.normal(0, 2, n_samples))
    supply_demand_ratio = np.clip(supply_demand_ratio, 0.1, 30)
    
    # Surge pressure (inverse of supply/demand, with noise)
    surge_pressure = np.clip(1 - supply_demand_ratio / 10, 0, 1)
    surge_pressure += np.random.normal(0, 0.1, n_samples)
    surge_pressure = np.clip(surge_pressure, 0, 1)
    
    # Deadhead km (market inefficiency)
    deadhead_km_avg = np.random.lognormal(mean=0.5, sigma=0.5, size=n_samples)
    deadhead_km_avg = np.clip(deadhead_km_avg, 0, 10)
    
    # Available drivers (varies by time)
    base_drivers = 30 + 20 * np.sin(2 * np.pi * hour / 24 - np.pi/3)
    available_drivers = np.maximum(0, base_drivers + np.random.normal(0, 10, n_samples))
    available_drivers = available_drivers.astype(int)
    
    # Ride requests (varies by time, peaks during rush hours)
    rush_hour_factor = 1 + 0.5 * (np.abs(hour - 8) < 2) + 0.5 * (np.abs(hour - 18) < 2)
    base_requests = 10 * rush_hour_factor
    ride_requests = np.maximum(0, base_requests + np.random.normal(0, 5, n_samples))
    ride_requests = ride_requests.astype(int)
    
    # Generate ETA (target variable)
    # Base: ~2.5 min per km
    base_eta = trip_distance_km * 150
    
    # Supply effect: more supply = faster
    supply_effect = -30 * np.log1p(supply_demand_ratio)
    
    # Surge effect: high surge = longer wait (up to 5 min extra)
    surge_effect = surge_pressure * 300
    
    # Driver availability effect
    driver_effect = -1.5 * available_drivers
    
    # Demand effect
    demand_effect = 3 * ride_requests
    
    # Time of day effect (slower during rush hours)
    rush_effect = 60 * ((np.abs(hour - 8) < 2) | (np.abs(hour - 18) < 2))
    
    # Combine with realistic noise
    pickup_eta_seconds = (
        base_eta + supply_effect + surge_effect + 
        driver_effect + demand_effect + rush_effect
    )
    pickup_eta_seconds += np.random.normal(0, 45, n_samples)
    pickup_eta_seconds = np.clip(pickup_eta_seconds, 120, 3600)
    
    # Create DataFrame
    df = pd.DataFrame({
        "timestamp": timestamps,
        "trip_distance_km": np.round(trip_distance_km, 2),
        "supply_demand_ratio": np.round(supply_demand_ratio, 3),
        "surge_pressure": np.round(surge_pressure, 3),
        "deadhead_km_avg": np.round(deadhead_km_avg, 3),
        "available_drivers": available_drivers,
        "ride_requests": ride_requests,
        "pickup_eta_seconds": np.round(pickup_eta_seconds, 0).astype(int),
    })
    
    return df


def main(n_samples: int = 10000, output_path: str = "data/trips_madrid.parquet"):
    print(f"Generating {n_samples} synthetic trip records...")
    
    df = generate_synthetic_trips(n_samples)
    
    # Create output directory if needed
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to parquet
    df.to_parquet(output_file, index=False)
    print(f"Saved to {output_file}")
    
    # Print summary statistics
    print("\nDataset Summary:")
    print(f"  Records: {len(df)}")
    print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"\nFeature statistics:")
    print(df.describe().round(2))
    
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic trip data")
    parser.add_argument(
        "--samples", 
        type=int, 
        default=10000, 
        help="Number of samples to generate"
    )
    parser.add_argument(
        "--output", 
        default="data/trips_madrid.parquet", 
        help="Output parquet file path"
    )
    args = parser.parse_args()
    
    main(n_samples=args.samples, output_path=args.output)