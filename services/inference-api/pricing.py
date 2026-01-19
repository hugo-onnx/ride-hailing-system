import math

MAX_SURGE = 2.0
MIN_SURGE = 1.0

ALPHA = 0.8   # supply-demand weight
BETA = 0.2    # deadhead weight


def compute_price_multiplier(features_5m: dict) -> dict:
    """
    Compute dynamic price multiplier.
    Returns multiplier + explanation.
    """
    sd_ratio = features_5m.get("supply_demand_ratio", 1.0)
    deadhead = features_5m.get("deadhead_km_avg", 0.0)
    requests = features_5m.get("ride_requests", 0)

    multiplier = 1.0
    reasons = []

    # Supply-demand imbalance
    if sd_ratio < 1.0:
        delta = ALPHA * (1.0 - sd_ratio)
        multiplier += delta
        reasons.append(f"supply_demand_shortage(+{round(delta,2)})")

    # Deadhead inefficiency
    if deadhead > 1.5:
        delta = BETA * math.log(deadhead)
        multiplier += delta
        reasons.append(f"deadhead_compensation(+{round(delta,2)})")

    # Low confidence dampening
    if requests < 5:
        multiplier = 1.0 + (multiplier - 1.0) * 0.5
        reasons.append("low_volume_dampening")

    # Clamp bounds
    multiplier = max(MIN_SURGE, min(MAX_SURGE, multiplier))

    # Determine surge level for display
    if multiplier >= 1.5:
        surge_level = "high"
    elif multiplier >= 1.2:
        surge_level = "moderate"
    else:
        surge_level = "normal"

    return {
        "multiplier": round(multiplier, 3),
        "surge_level": surge_level,
        "reasons": reasons,
    }
