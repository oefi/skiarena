#!/usr/bin/env python3
"""
Zwei Länder Skiarena — High-Resolution Alpine Forecast Fetcher
Fetches both hourly (for canvas charts) and daily (for tactical board cards).
best_match blends ICON-D2/AROME/HARMONIE for days 1-5, then ECMWF IFS/GFS.
"""

import requests, json, sys, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

OUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "forecast_data.json"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
BASE_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m", "apparent_temperature", "precipitation", "rain",
    "snowfall", "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "visibility", "freezing_level_height",
    "cloud_cover",
    "cloud_cover_low",          # stratus/fog layer — lift closures, whiteout risk
    "cloud_cover_mid",          # altostratus — diffuse flat light
    "cloud_cover_high",         # cirrus — harmless, fine skiing
    "sunshine_duration",
    "direct_radiation",         # W/m² unscattered beam — truest bluebird intensity metric
    "soil_temperature_0cm",
    "snow_depth",               # needed for forecast Bluebird Score (25% weight)
    "precipitation_probability", # essential for trip planning confidence
]

# Daily aggregates — used directly in tactical board cards, avoids JS re-derivation.
# NOTE: freezing_level_height has NO daily aggregate in the forecast API.
# The JS derives per-day freeze-level stats from hourly.freezing_level_height.
DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "snowfall_sum",
    "rain_sum",
    "precipitation_sum",
    "sunshine_duration",
    "wind_gusts_10m_max",
    "wind_speed_10m_max",
    "weather_code",
    "precipitation_hours",
    "uv_index_max",             # altitude UV exposure — critical at 2750-3250m summit
]

FORECAST_DAYS = 16

RESORTS = [
    ("nauders",    46.88, 10.50, 2750),
    ("schoeneben", 46.80, 10.48, 2390),
    ("watles",     46.70, 10.50, 2550),
    ("sulden",     46.52, 10.58, 3250),
    ("trafoi",     46.55, 10.50, 2800)
]

def get_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def main():
    session = get_session()
    forecast_payload = {
        "_meta": {
            "source": "Open-Meteo Best Match",
            "forecast_days": FORECAST_DAYS,
            "hourly_vars": HOURLY_VARS,
            "daily_vars": DAILY_VARS,
        },
        "resorts": {}
    }

    try:
        for name, lat, lon, elev in RESORTS:
            params = {
                "latitude": lat,
                "longitude": lon,
                "elevation": elev,
                "hourly": ",".join(HOURLY_VARS),
                "daily": ",".join(DAILY_VARS),
                "models": "best_match",
                "forecast_days": FORECAST_DAYS,
                "timezone": "Europe/Berlin"
            }
            resp = session.get(BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            forecast_payload["resorts"][name] = {
                "hourly": data.get("hourly", {}),
                "daily":  data.get("daily",  {}),
                "hourly_units": data.get("hourly_units", {}),
                "daily_units":  data.get("daily_units",  {}),
            }
            time.sleep(0.5)

        with open(OUT_FILE, "w") as f:
            json.dump(forecast_payload, f, separators=(",", ":"))

    except Exception as e:
        print(f"\n[!] WARNING: High-res forecast fetch failed: {e}")
        with open(OUT_FILE, "w") as f:
            json.dump({"error": str(e), "resorts": {}}, f)

if __name__ == "__main__":
    main()

