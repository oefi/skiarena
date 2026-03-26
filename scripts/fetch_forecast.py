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
from datetime import datetime, timezone

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
    # DST NOTE: all hourly timestamps are in Europe/Berlin local time (CET/CEST).
    # On the DST spring-forward day (~last Sunday of March) Open-Meteo returns only
    # 23 hourly entries — the 02:xx hour does not exist in local time.
    # JS consumers MUST use the stored utc_offset_seconds to reconstruct absolute
    # times: treat timestamps as local and apply the offset, or use Unix epoch.
    # Do NOT assume 24 entries per day in March/April hourly arrays.
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
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
                    # Preserve UTC offset info from the API response so JS chart code
                    # can correctly handle DST transitions (spring-forward = 23-hour day
                    # in late March; fall-back = 25-hour day in late October).
                    # utc_offset_seconds reflects the offset at fetch time; during DST
                    # transitions within the 16-day window the actual offset may shift
                    # (CET=+3600 → CEST=+7200). JS must add this to timestamp math.
                    "utc_offset_seconds":   data.get("utc_offset_seconds"),
                    "timezone_abbreviation": data.get("timezone_abbreviation"),
                }

                # Aggregate hourly snow_depth → daily MAX, mirroring fetch_openmeteo.py.
                # The forecast API has no daily snow_depth aggregate; hourly is the only source.
                # MAX is ski-conservative: captures peak depth for the day without the
                # undercount of a midday or mean reading on melt/accumulation days.
                # daily.sunshine_duration is in seconds (unit "s") — same as archive API.
                h_times = data.get("hourly", {}).get("time", [])
                h_depth = data.get("hourly", {}).get("snow_depth", [])
                d_times = data.get("daily",  {}).get("time", [])
                if h_times and h_depth and d_times:
                    depth_by_date: dict[str, float] = {}
                    for ts, v in zip(h_times, h_depth):
                        if v is not None:
                            day = ts[:10]
                            if day not in depth_by_date or v > depth_by_date[day]:
                                depth_by_date[day] = v
                    resort_payload[label]["daily"]["snow_depth"] = [
                        depth_by_date.get(d) for d in d_times
                    ]
                    resort_payload[label]["daily_units"]["snow_depth"] = (
                        data.get("hourly_units", {}).get("snow_depth", "m")
                    )
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

