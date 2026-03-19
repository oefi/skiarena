#!/usr/bin/env python3
"""
Zwei Länder Skiarena — High-Resolution Alpine Forecast Fetcher
Fetches both hourly (for canvas charts) and daily (for tactical board cards).
best_match blends ICON-D2/AROME/HARMONIE for days 1-5, then ECMWF IFS/GFS.
"""

import requests, json, sys, time, os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

OUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "forecast_data.json"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
BASE_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m", "precipitation", "rain",
    "snowfall", "weather_code",
    "wind_gusts_10m",
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
    "precipitation_probability_max",  # peak daily precip prob — eliminates JS hourly re-derivation
]

FORECAST_DAYS = 16

# (name, lat, lon, summit_elev, base_elev)
# base uses same lat/lon — Open-Meteo lapse-adjusts via the elevation param.
RESORTS = [
    ("nauders",    46.88, 10.50, 2750, 1400),
    ("schoeneben", 46.80, 10.48, 2390, 1460),
    ("watles",     46.70, 10.50, 2550, 1500),
    ("sulden",     46.52, 10.58, 3250, 1900),
    ("trafoi",     46.55, 10.50, 2800, 1540),
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
            "elevations": "summit + base per resort",
        },
        "resorts": {}
    }

    try:
        for name, lat, lon, summit_elev, base_elev in RESORTS:
            resort_payload = {}
            for label, elev in (("summit", summit_elev), ("base", base_elev)):
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
                resort_payload[label] = {
                    "hourly": data.get("hourly", {}),
                    "daily":  data.get("daily",  {}),
                    "hourly_units": data.get("hourly_units", {}),
                    "daily_units":  data.get("daily_units",  {}),
                }
                time.sleep(0.5)
            forecast_payload["resorts"][name] = resort_payload

        # Validate before committing — don't write if all resorts failed
        if not any(forecast_payload["resorts"].values()):
            raise ValueError("All resort fetches returned empty data")
        tmp = OUT_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(forecast_payload, f, separators=(",", ":"))
        tmp.replace(OUT_FILE)

    except Exception as e:
        print(f"\n[!] WARNING: High-res forecast fetch failed: {e}")
        if OUT_FILE.exists():
            print(f"    Retaining cached {OUT_FILE.name} for dashboard build.")
        else:
            print(f"    No cached forecast exists — dashboard will show no forecast data.")
        sys.exit(1)  # signal failure to action_refresh.py (allow_fail=True handles it)

if __name__ == "__main__":
    main()

