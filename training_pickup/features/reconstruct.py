import pandas as pd


def reconstruct_features(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Reconstruct ETA features from historical trip records.
    Assumes features were already logged per 5m window.
    
    Args:
        trips: DataFrame with trip records including marketplace features
    
    Returns:
        Cleaned DataFrame with features and labels ready for training
    """
    features = trips[[
        "trip_distance_km",
        "supply_demand_ratio",
        "surge_pressure",
        "deadhead_km_avg",
        "available_drivers",
        "ride_requests",
        "pickup_eta_seconds",
    ]].dropna()

    features["pickup_eta_seconds"] = features[
        "pickup_eta_seconds"
    ].clip(120, 3600)

    return features