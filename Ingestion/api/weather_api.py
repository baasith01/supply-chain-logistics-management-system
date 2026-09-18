"""
Weather API ingestion module.

Purpose:
    Fetch current weather data for a location from a weather API and
    normalize the response into a DataFrame suitable for the logistics
    pipeline.

Configuration:
    WEATHER_API_KEY is read from the environment.
    The API URL can be overridden with WEATHER_API_URL.

This module is designed for API ingestion. The project can also use the
prepared weather.csv dataset when a live API is not available.
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

DEFAULT_API_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather(
    location: str,
    api_key: Optional[str] = None,
    api_url: Optional[str] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """
    Fetch current weather information for one location.

    Args:
        location: City/location name accepted by the weather API.
        api_key: API key. If omitted, WEATHER_API_KEY is used.
        api_url: API endpoint. If omitted, WEATHER_API_URL or the default
                 OpenWeather endpoint is used.
        timeout: HTTP request timeout in seconds.

    Returns:
        Parsed JSON response from the API.

    Raises:
        ValueError: If the API key is missing or the response is invalid.
        requests.HTTPError: If the API returns an HTTP error.
        requests.RequestException: For network-related failures.
    """
    api_key = api_key or os.getenv("WEATHER_API_KEY")
    api_url = api_url or os.getenv("WEATHER_API_URL", DEFAULT_API_URL)

    if not api_key or api_key == "your_weather_api_key_here":
        raise ValueError(
            "Weather API key is missing. Set WEATHER_API_KEY in your .env file."
        )

    params = {
        "q": location,
        "appid": api_key,
        "units": "metric",
    }

    response = requests.get(api_url, params=params, timeout=timeout)
    response.raise_for_status()

    data = response.json()

    if "main" not in data or "coord" not in data:
        raise ValueError("Unexpected weather API response format.")

    return data


def normalize_weather(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert an API response into the project's standardized weather schema.

    Returns columns compatible with data/raw/weather.csv and
    data/processed/weather_clean.csv.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": timestamp,
        "location": data.get("name"),
        "latitude": data.get("coord", {}).get("lat"),
        "longitude": data.get("coord", {}).get("lon"),
        "temperature_c": data.get("main", {}).get("temp"),
        "humidity_pct": data.get("main", {}).get("humidity"),
        "rainfall_mm": data.get("rain", {}).get("1h", 0),
        "weather_condition": (
            data.get("weather", [{}])[0].get("description", "Unknown").title()
        ),
    }

    return pd.DataFrame([row])


def save_weather(
    location: str,
    output_path: str = "data/raw/weather_api_latest.csv",
) -> pd.DataFrame:
    """
    Fetch, normalize, and save the latest weather observation.
    """
    data = fetch_weather(location)
    df = normalize_weather(data)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)

    return df


if __name__ == "__main__":
    from pathlib import Path

    location = os.getenv("WEATHER_LOCATION", "Chennai")

    try:
        result = save_weather(location)
        print(f"Weather data saved for {location}:")
        print(result.to_string(index=False))
    except (ValueError, requests.RequestException) as exc:
        print(f"Weather API ingestion failed: {exc}")
