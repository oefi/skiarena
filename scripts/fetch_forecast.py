#!/usr/bin/env python3
"""
Zwei Länder Skiarena — High-Resolution Alpine Forecast Fetcher
Hits the Open-Meteo forecast API using MeteoSwiss (ICON-CH1/CH2) and DWD models.
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
    "snowfall", "weathercode", "windspeed_10m", "windgusts_10m", 
    "visibility", "freezinglevel_height"
]
MODELS = ["meteoswiss_seamless", "icon_seamless"]
FORECAST_DAYS = 3

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
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def main():
    session = get_session()
    forecast_payload = {
        "_meta": {"source": "Open-Meteo MeteoSwiss", "forecast_days": FORECAST_DAYS},
        "resorts": {}
    }

    try:
        for name, lat, lon, elev in RESORTS:
            params = {
                "latitude": lat, 
                "longitude": lon, 
                "elevation": elev,
                "hourly": ",".join(HOURLY_VARS), 
                "models": ",".join(MODELS),
                "forecast_days": FORECAST_DAYS, 
                "timezone": "Europe/Berlin"
            }
            resp = session.get(BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            forecast_payload["resorts"][name] = resp.json().get("hourly", {})
            time.sleep(0.5)
            
        with open(OUT_FILE, "w") as f:
            json.dump(forecast_payload, f, separators=(",", ":"))
            
    except Exception as e:
        print(f"\n[!] WARNING: High-res forecast fetch failed: {e}")
        with open(OUT_FILE, "w") as f:
            json.dump({"_meta": {"error": str(e)}, "resorts": {}}, f)

if __name__ == "__main__":
    main()