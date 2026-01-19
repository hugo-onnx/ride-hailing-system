import joblib
import numpy as np

ETA_MIN_S = 120
ETA_MAX_S = 3600


class PickupETAEstimator:
    """
    Pickup ETA estimation model using XGBoost.
    
    Predicts estimated time of arrival based on trip distance
    and marketplace features (supply/demand conditions).
    """
    
    def __init__(self, model_path: str):
        """Load pre-trained model from disk."""
        self.model = joblib.load(model_path)

    def predict(self, features: dict) -> float:
        """
        Predict ETA in seconds.
        
        Args:
            features: Dictionary containing:
                - trip_distance_km
                - supply_demand_ratio
                - surge_pressure
                - deadhead_km_avg
                - available_drivers
                - ride_requests
        
        Returns:
            Estimated time in seconds, clamped to [ETA_MIN, ETA_MAX]
        """
        x = np.array([[ 
            features["trip_distance_km"],
            features["supply_demand_ratio"],
            features["surge_pressure"],
            features["deadhead_km_avg"],
            features["available_drivers"],
            features["ride_requests"],
        ]])

        eta = float(self.model.predict(x)[0])
        return max(ETA_MIN_S, min(ETA_MAX_S, eta))