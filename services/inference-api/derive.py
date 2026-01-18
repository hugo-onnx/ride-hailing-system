def safe_div(numerator: float, denominator: float) -> float:
    """Safe division that returns 0.0 when denominator is zero."""
    return numerator / denominator if denominator > 0 else 0.0


def derive_features(raw: dict) -> dict:
    """
    Compute derived marketplace features from raw Redis data.
    
    Input (raw Redis hash):
        - ride_requests: int
        - available_drivers: int
        - active_drivers: int
        - deadhead_km_sum: float
        - idle_events: int
    
    Output (derived features):
        - ride_requests: demand signal
        - available_drivers: supply signal
        - deadhead_km_avg: average idle driving distance (market inefficiency)
        - supply_demand_ratio: available_drivers / ride_requests
        - surge_pressure: 0-1 score indicating supply shortage
    """
    ride_requests = int(raw.get("ride_requests", 0))
    available_drivers = int(raw.get("available_drivers", 0))
    active_drivers = int(raw.get("active_drivers", 0))
    deadhead_sum = float(raw.get("deadhead_km_sum", 0.0))
    idle_events = int(raw.get("idle_events", 0))

    # Average deadhead distance per idle event
    deadhead_avg = safe_div(deadhead_sum, idle_events)
    
    # Supply/demand ratio (higher = more supply available)
    supply_demand_ratio = safe_div(available_drivers, ride_requests)

    # If ratio is 3.0 or higher, pressure is 0. 
    # If ratio drops toward 0, pressure approaches 1.
    target_ratio = 3.0
    surge_pressure = max(0.0, min(1.0, (target_ratio - supply_demand_ratio) / target_ratio))

    return {
        "ride_requests": ride_requests,
        "available_drivers": available_drivers,
        "active_drivers": active_drivers,
        "deadhead_km_avg": round(deadhead_avg, 3),
        "supply_demand_ratio": round(supply_demand_ratio, 3),
        "surge_pressure": round(surge_pressure, 3),
    }