GAMMA = 0.6


def adjust_dropoff_eta(
    osrm_duration_s: float,
    surge_pressure: float,
    gamma: float = GAMMA,
) -> float:
    """
    Adjust OSRM free-flow duration based on market congestion.
    
    Formula:
        adjusted_eta = osrm_duration * (1 + γ * surge_pressure)
    
    The surge_pressure (0-1) indicates supply shortage in the market,
    which correlates with traffic congestion during high-demand periods.
    
    Args:
        osrm_duration_s: Free-flow duration from OSRM in seconds
        surge_pressure: Market pressure indicator (0-1)
        gamma: Congestion sensitivity factor (default 0.6)
    
    Returns:
        Adjusted duration in seconds
    
    Example:
        - osrm_duration_s=600, surge_pressure=0.0 → 600s (no adjustment)
        - osrm_duration_s=600, surge_pressure=0.5 → 780s (30% increase)
        - osrm_duration_s=600, surge_pressure=1.0 → 960s (60% increase)
    """
    factor = 1.0 + gamma * surge_pressure
    return osrm_duration_s * factor


def compute_congestion_factor(surge_pressure: float, gamma: float = GAMMA) -> float:
    """
    Compute the congestion multiplier for a given surge pressure.
    
    Useful for displaying the congestion impact to users.
    
    Args:
        surge_pressure: Market pressure indicator (0-1)
        gamma: Congestion sensitivity factor
    
    Returns:
        Multiplier (1.0 to 1+gamma)
    """
    return 1.0 + gamma * surge_pressure