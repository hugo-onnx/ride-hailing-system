import joblib
import numpy as np

MIN_SPEED_KMH = 8
MAX_SPEED_KMH = 45


class DropoffETAEstimator:
    """
    Dropoff ETA estimation model.
    
    Predicts average travel speed based on trip distance, surge conditions,
    time of day, and whether it's a weekend. Then calculates ETA from speed.
    """
    
    def __init__(self, model_path: str):
        """Load pre-trained speed prediction model."""
        self.model = joblib.load(model_path)

    def predict(self, features: dict) -> float:
        """
        Predict dropoff ETA in seconds.
        
        Args:
            features: Dictionary containing:
                - trip_distance_km: Trip distance
                - surge_pressure: Current surge level (0-1)
                - hour_of_day: Hour (0-23)u
                - is_weekend: Boolean or int (0/1)
        
        Returns:
            Estimated dropoff time in seconds
        """
        x = np.array([[
            features["trip_distance_km"],
            features["surge_pressure"],
            features["hour_of_day"],
            int(features["is_weekend"]),
        ]])

        speed_kmh = float(self.model.predict(x)[0])
        speed_kmh = max(MIN_SPEED_KMH, min(MAX_SPEED_KMH, speed_kmh))

        eta_hours = features["trip_distance_km"] / speed_kmh
        return eta_hours * 3600