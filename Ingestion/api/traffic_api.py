"""
Traffic API ingestion module.

Purpose:
    Fetch traffic information for a location/route from a traffic API and
    normalize the response into the logistics pipeline schema.

Configuration:
    TRAFFIC_API_KEY is read from the environment.
    TRAFFIC_API_URL can override the default endpoint.

Note:
    Traffic APIs differ significantly in their response formats. This module
    keeps the provider-specific request small and converts the common fields
    needed by this project into a standard DataFrame.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

DEFAULT_API_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def fetch_traffic(
    latitude: float,
    longitude: float,
    api_key: Optional[str] = None,
    api_url: Optional[str] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """
    Fetch current traffic information near a latitude/longitude.

    Args:
        latitude: Location latitude.
        longitude: Location longitude.
        api_key: API key. If omitted, TRAFFIC_API_KEY is used.
        api_url: Traffic API endpoint.
        timeout: HTTP request timeout in seconds.

    Returns:
        Parsed JSON response.

    Raises:
        ValueError: If the API key is missing or the response is invalid.
        requests.RequestException: For network/API failures.
    """
    api_key = api_key or os.getenv("TRAFFIC_API_KEY")
    api_url = api_url or os.getenv("TRAFFIC_API_URL", DEFAULT_API_URL)

    if not api_key or api_key == "your_traffic_api_key_here":
        raise ValueError(
            "Traffic API key is missing. Set TRAFFIC_API_KEY in your .env file."
        )

    params = {
        "point": f"{latitude},{longitude}",
        "key": api_key,
    }

    response = requests.get(api_url, params=params, timeout=timeout)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, dict):
        raise ValueError("Unexpected traffic API response format.")

    return data


def normalize_traffic(
    data: Dict[str, Any],
    latitude: float,
    longitude: float,
    location: str = "Unknown",
) -> pd.DataFrame:
    """
    Convert a traffic-provider response into the project's standard schema.

    Standard output:
        timestamp, location, latitude, longitude,
        traffic_score, traffic_level, average_speed_kmph
    """
    flow = data.get("flowSegmentData", {})

    current_speed = flow.get("currentSpeed")
    free_flow_speed = flow.get("freeFlowSpeed")

    current_speed = pd.to_numeric(current_speed, errors="coerce")
    free_flow_speed = pd.to_numeric(free_flow_speed, errors="coerce")

    # TomTom speeds are commonly returned in km/h for this endpoint.
    average_speed = current_speed

    if pd.notna(current_speed) and pd.notna(free_flow_speed) and free_flow_speed > 0:
        speed_ratio = current_speed / free_flow_speed
        traffic_score = max(0, min(100, round((1 - speed_ratio) * 100, 2)))
    else:
        traffic_score = None

    if traffic_score is None:
        traffic_level = "Unknown"
    elif traffic_score >= 70:
        traffic_level = "Severe"
    elif traffic_score >= 40:
        traffic_level = "Heavy"
    elif traffic_score >= 20:
        traffic_level = "Moderate"
    else:
        traffic_level = "Low"

    row = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "location": location,
        "latitude": latitude,
        "longitude": longitude,
        "traffic_score": traffic_score,
        "traffic_level": traffic_level,
        "average_speed_kmph": average_speed,
    }

    return pd.DataFrame([row])


def save_traffic(
    latitude: float,
    longitude: float,
    location: str = "Unknown",
    output_path: str = "data/raw/traffic_api_latest.csv",
) -> pd.DataFrame:
    """
    Fetch, normalize, and save the latest traffic observation.
    """
    data = fetch_traffic(latitude, longitude)
    df = normalize_traffic(data, latitude, longitude, location)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)

    return df


if __name__ == "__main__":
    try:
        latitude = float(os.getenv("TRAFFIC_LATITUDE", "13.0827"))
        longitude = float(os.getenv("TRAFFIC_LONGITUDE", "80.2707"))
        location = os.getenv("TRAFFIC_LOCATION", "Chennai")

        result = save_traffic(
            latitude=latitude,
            longitude=longitude,
            location=location,
        )

        print(f"Traffic data saved for {location}:")
        print(result.to_string(index=False))

    except (ValueError, requests.RequestException) as exc:
        print(f"Traffic API ingestion failed: {exc}")
