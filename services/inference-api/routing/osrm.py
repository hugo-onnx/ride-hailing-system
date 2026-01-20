import os
import logging
import requests
from dataclasses import dataclass
from typing import Tuple

logger = logging.getLogger(__name__)

OSRM_HOST = os.getenv("OSRM_HOST", "http://osrm:5000")
OSRM_TIMEOUT = float(os.getenv("OSRM_TIMEOUT", "2.0"))


@dataclass
class RouteResult:
    """Result from OSRM route calculation."""
    distance_m: float
    duration_s: float
    
    @property
    def distance_km(self) -> float:
        return self.distance_m / 1000
    
    @property
    def duration_min(self) -> float:
        return self.duration_s / 60


class OSRMError(Exception):
    """Exception raised when OSRM request fails."""
    pass


class OSRMClient:
    """
    Client for OSRM routing API.
    
    Calculates road network distance and free-flow duration
    between two points using OSRM's route service.
    """
    
    def __init__(self, host: str = OSRM_HOST, timeout: float = OSRM_TIMEOUT):
        self.host = host.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
    
    def get_route(
        self,
        origin: Tuple[float, float],
        destination: Tuple[float, float],
    ) -> RouteResult:
        """
        Calculate route between two points.
        
        Args:
            origin: (longitude, latitude) of origin
            destination: (longitude, latitude) of destination
        
        Returns:
            RouteResult with distance_m and duration_s
        
        Raises:
            OSRMError: If OSRM request fails or returns no routes
        """
        coords = f"{origin[0]},{origin[1]};{destination[0]},{destination[1]}"
        url = f"{self.host}/route/v1/driving/{coords}"
        
        params = {
            "overview": "false",
            "annotations": "false",
        }
        
        try:
            resp = self._session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.Timeout:
            logger.error(f"OSRM request timed out after {self.timeout}s")
            raise OSRMError("OSRM request timed out")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"OSRM connection failed: {e}")
            raise OSRMError(f"OSRM connection failed: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"OSRM HTTP error: {e}")
            raise OSRMError(f"OSRM HTTP error: {e}")
        
        if data.get("code") != "Ok":
            error_msg = data.get("message", "Unknown OSRM error")
            logger.warning(f"OSRM error: {error_msg}")
            raise OSRMError(f"OSRM error: {error_msg}")
        
        routes = data.get("routes", [])
        if not routes:
            raise OSRMError("No route found between points")
        
        route = routes[0]
        
        return RouteResult(
            distance_m=route["distance"],
            duration_s=route["duration"],
        )
    
    def health_check(self) -> bool:
        """
        Check if OSRM service is available.
        
        Returns:
            True if OSRM is responding, False otherwise
        """
        try:
            resp = self._session.get(
                f"{self.host}/route/v1/driving/-3.7038,40.4168;-3.7038,40.4169",
                timeout=self.timeout,
            )
            return resp.status_code == 200
        except Exception:
            return False


_client: OSRMClient | None = None


def get_osrm_client() -> OSRMClient:
    """Get or create the OSRM client singleton."""
    global _client
    if _client is None:
        _client = OSRMClient()
    return _client


def get_route(
    origin: Tuple[float, float],
    destination: Tuple[float, float],
) -> RouteResult:
    """
    Convenience function to get a route.
    
    Args:
        origin: (longitude, latitude) of origin
        destination: (longitude, latitude) of destination
    
    Returns:
        RouteResult with distance and duration
    """
    return get_osrm_client().get_route(origin, destination)