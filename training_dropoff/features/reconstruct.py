import pandas as pd


def reconstruct_features(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Reconstruct dropoff ETA features from historical trip records.
    
    Args:
        trips: DataFrame with trip records including marketplace features
    
    Returns:
        Cleaned DataFrame with features and labels ready for training
    """
    features = trips[[
        "trip_distance_km",
        "surge_pressure",
        "hour_of_day",
        "is_weekend",
        "dropoff_eta_seconds",
    ]].dropna()
    
    features["hour_of_day"] = features["hour_of_day"].astype(int)
    features["is_weekend"] = features["is_weekend"].astype(int)
    
    features["dropoff_eta_seconds"] = features["dropoff_eta_seconds"].clip(120, 7200)

    return features