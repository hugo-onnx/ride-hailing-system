import math


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Calculate the great-circle distance between two points on Earth.
    
    Uses the Haversine formula to compute distance in kilometers.
    
    Args:
        lat1, lng1: Origin coordinates (degrees)
        lat2, lng2: Destination coordinates (degrees)
    
    Returns:
        Distance in kilometers
    """
    R = 6371.0  # Earth's radius in km
    
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    
    return 2 * R * math.asin(math.sqrt(a))