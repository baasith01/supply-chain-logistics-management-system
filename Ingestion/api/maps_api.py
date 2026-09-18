"""
Maps / routing API ingestion module.

Purpose:
    Retrieve route and travel-time information for logistics planning.
    The module is provider-agnostic at the business-logic level and uses
    OpenRouteService by default.

Configuration:
    MAPS_API_KEY is read from the environment.
    MAPS_API_URL can override the default routing endpoint.

The normalized output can be used by route analysis, delivery prediction,
and operational dashboards.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

DEFAULT_API_URL = "https://api.openrouteservice.org/v2/directions/driving-car"


def fetch_route(
    start: Tuple[float, float],
    end: Tuple[float, float],
    api_key: Optional[str] = None,
    api_url: Optional[str] = None,
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    Fetch a driving route between two coordinates.

    Coordinates use (longitude, latitude), which is the format expected
    by OpenRouteService.

    Args:
        start: Starting point as (longitude, latitude).
        end: Destination as (longitude, latitude).
        api_key: API key. If omitted, MAPS_API_KEY is used.
        api_url: Routing API endpoint.
        timeout: HTTP request timeout in seconds.

    Returns:
        Parsed JSON response.

    Raises:
        ValueError: If the API key is missing or response format is invalid.
        requests.RequestException: For network/API failures.
    """
    api_key = api_key or os.getenv("MAPS_API_KEY")
    api_url = api_url or os.getenv("MAPS_API_URL", DEFAULT_API_URL)

    if not api_key or api_key == "your_maps_api_key_here":
        raise ValueError(
            "Maps API key is missing. Set MAPS_API_KEY in your .env file."
        )

    payload = {
        "coordinates": [
            [float(start[0]), float(start[1])],
            [float(end[0]), float(end[1])],
        ]
    }

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    response = requests.post(
        api_url,
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, dict) or "features" not in data:
        raise ValueError("Unexpected maps API response format.")

    return data


def normalize_route(
    data: Dict[str, Any],
    start: Tuple[float, float],
    end: Tuple[float, float],
) -> pd.DataFrame:
    """
    Normalize route information into a compact logistics schema.

    Output fields:
        timestamp
        start_longitude, start_latitude
        end_longitude, end_latitude
        distance_km
        estimated_duration_min
    """
    features = data.get("features", [])

    if not features:
        raise ValueError("No route found for the requested coordinates.")

    properties = features[0].get("properties", {})
    summary = properties.get("summary", {})

    distance_m = pd.to_numeric(summary.get("distance"), errors="coerce")
    duration_s = pd.to_numeric(summary.get("duration"), errors="coerce")

    row = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "start_longitude": start[0],
        "start_latitude": start[1],
        "end_longitude": end[0],
        "end_latitude": end[1],
        "distance_km": (
            round(float(distance_m) / 1000, 2)
            if pd.notna(distance_m)
            else None
        ),
        "estimated_duration_min": (
            round(float(duration_s) / 60, 2)
            if pd.notna(duration_s)
            else None
        ),
    }

    return pd.DataFrame([row])


def save_route(
    start: Tuple[float, float],
    end: Tuple[float, float],
    output_path: str = "data/raw/maps_api_latest.csv",
) -> pd.DataFrame:
    """
    Fetch, normalize, and save route information.
    """
    data = fetch_route(start, end)
    df = normalize_route(data, start, end)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)

    return df


if __name__ == "__main__":
    try:
        start = (
            float(os.getenv("MAPS_START_LONGITUDE", "80.2707")),
            float(os.getenv("MAPS_START_LATITUDE", "13.0827")),
        )
        end = (
            float(os.getenv("MAPS_END_LONGITUDE", "80.6480")),
            float(os.getenv("MAPS_END_LATITUDE", "13.0827")),
        )

        result = save_route(start=start, end=end)

        print("Route data:")
        print(result.to_string(index=False))

    except (ValueError, requests.RequestException) as exc:
        print(f"Maps API ingestion failed: {exc}")
