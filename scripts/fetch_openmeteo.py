#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher

Dual-call strategy per elevation:
  Call A — ERA5 best-match (no elevation param): all daily weather variables.
            sunshine_duration, shortwave_radiation_sum, snowfall_sum, temperature,
            wind, precipitation, weather_code are all available as daily aggregates.
  Call B — ERA5-Land (elevation + models=era5_land): snow_depth HOURLY.
            snow_depth exists ONLY in ERA5-Land. It has NO daily aggregate in the
            archive API — we request hourly and compute the daily mean ourselves.
            The elevation param enables altitude-corrected values.
  Merge  — aggregate hourly snow_depth to daily mean, inject into Call A's daily
            dict under the same key before saving.

This eliminates the sunshine_duration inference band-aid in clean_normalize
(ERA5-Land routinely drops it; ERA5 always has it) and gives accurate snowfall
from ERA5 while keeping altitude-correct snow depth from ERA5-Land.

ERA5-Land note: publishing lag is 5–7 days. The ERA5LagError handler detects the
400 "Data only available until …" response and retries with the real cutoff,
caching it for subsequent resort fetches.
"""

import requests, json, time, sys, argparse, subprocess, re
from collections import defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import date, timedelta, datetime

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL   = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2019-11-01"
ERA5_LAG_DAYS = 7

# Call A — ERA5 grid (no elevation). All variables except snow_depth.
ERA5_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_min",
    "snowfall_sum",
    "precipitation_sum",
    "precipitation_hours",
    "sunshine_duration",
    "shortwave_radiation_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "weather_code",
]

RESORTS = [
    ("nauders",    46.88, 10.50, 1400, 2750),
    ("schoeneben", 46.80, 10.48, 1460, 2390),
    ("watles",     46.70, 10.50, 1500, 2550),
    ("sulden",     46.52, 10.58, 1900, 3250),
    ("trafoi",     46.55, 10.50, 1540, 2800)
]

# Shared ERA5-Land cutoff discovered at runtime — avoids one extra round-trip per resort
_discovered_end_date = None


def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class ERA5LagError(Exception):
    def __init__(self, available_until: str):
        self.available_until = available_until
        super().__init__(f"ERA5-Land data only available until {available_until}")


def _get(session, params):
    """GET with body-aware error handling. Returns parsed JSON or raises."""
    resp = session.get(BASE_URL, params=params, timeout=15)
    try:
        body = resp.json()
    except Exception:
        body = {}
    if resp.status_code == 400:
        reason = body.get("reason", "")
        if "only available until" in reason:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", reason)
            if m:
                raise ERA5LagError(m.group(1))
        resp.raise_for_status()
    resp.raise_for_status()
    # Also catch 200-with-error-body (Open-Meteo sometimes does this)
    if body.get("error") and "only available until" in body.get("reason", ""):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", body["reason"])
        if m:
            raise ERA5LagError(m.group(1))
    # Guard against empty returns (200 but no data).
    # snow_depth Call B now returns hourly, so check both keys.
    n_daily  = len(body.get("daily",  {}).get("time", []))
    n_hourly = len(body.get("hourly", {}).get("time", []))
    if n_daily == 0 and n_hourly == 0:
        raise ValueError(f"API returned 200 but zero records for params: {params}")
    return body


def fetch_merged(session, name, lat, lon, elevation_label, start_d, end_d, elevation_m):
    """
    Perform Call A (ERA5, no elevation) + Call B (ERA5-Land, elevation for snow_depth),
    inject snow_depth from B into A's daily dict, save one merged JSON file.
    Returns number of days fetched.
    """
    base_params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_d, "end_date": end_d,
        "daily": ",".join(ERA5_VARS),
        "timezone": "Europe/Berlin",
    }
    depth_params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_d, "end_date": end_d,
        "hourly": "snow_depth",      # snow_depth is HOURLY-ONLY in the archive API
        "timezone": "Europe/Berlin",
        "elevation": elevation_m,
        "models": "era5_land",       # snow_depth only exists in ERA5-Land
    }

    # Call A — ERA5
    era5_data  = _get(session, base_params)

    # Call B — ERA5-Land (snow_depth). Use same end_date; lag handled by caller.
    depth_data = _get(session, depth_params)

    # Merge: aggregate hourly snow_depth → daily mean, inject into ERA5 daily dict.
    # ERA5-Land returns hourly data; we compute the daily mean of non-null values.
    # This gives a representative "average snowpack depth" for the day.
    era5_dates   = era5_data["daily"].get("time", [])
    hourly_times = depth_data["hourly"].get("time", [])
    hourly_depth = depth_data["hourly"].get("snow_depth", [])

    daily_depth_raw: dict = defaultdict(list)
    for ts, v in zip(hourly_times, hourly_depth):
        if v is not None:
            daily_depth_raw[ts[:10]].append(v)

    depth_by_date = {
        d: sum(vs) / len(vs) for d, vs in daily_depth_raw.items() if vs
    }

    if len(era5_dates) != len(depth_by_date):
        print(f"  [!] WARNING: ERA5 ({len(era5_dates)} days) and ERA5-Land "
              f"({len(depth_by_date)} days aggregated) date ranges differ for "
              f"{name}/{elevation_label}. snow_depth will be None for gaps.")

    era5_data["daily"]["snow_depth"] = [
        depth_by_date.get(d) for d in era5_dates
    ]
    # Carry over unit declaration from ERA5-Land hourly response
    if "daily_units" in era5_data and "hourly_units" in depth_data:
        era5_data["daily_units"]["snow_depth"] = depth_data["hourly_units"].get("snow_depth", "m")

    if name != "probe":
        out_file = OUTPUT_DIR / f"{name}_{elevation_label}_raw.json"
        with open(out_file, "w") as f:
            json.dump(era5_data, f)

    return len(era5_dates)


def default_end_date():
    return (date.today() - timedelta(days=ERA5_LAG_DAYS)).strftime("%Y-%m-%d")


def main():
    global _discovered_end_date

    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args()

    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"[!] Invalid --end-date '{args.end_date}'. Expected YYYY-MM-DD.")

    end_date = args.end_date or default_end_date()
    if end_date < START_DATE:
        sys.exit(f"[!] --end-date ({end_date}) cannot be before START_DATE ({START_DATE}).")

    session = get_session()

    if args.probe:
        try:
            fetch_merged(session, "probe", RESORTS[0][1], RESORTS[0][2],
                         "test", "2024-01-01", "2024-01-02", RESORTS[0][3])
            print("Probe OK — all variables accessible.")
        except Exception as e:
            sys.exit(f"Probe failed: {e}")
        return

    try:
        for name, lat, lon, base_m, summit_m in RESORTS:
            print(f"\n[{name.upper()}]")
            for elev_label, elev_m in [("base", base_m), ("summit", summit_m)]:
                effective_end = _discovered_end_date or end_date
                try:
                    n = fetch_merged(session, name, lat, lon, elev_label,
                                     START_DATE, effective_end, elev_m)
                    print(f"  ✓ {elev_label.capitalize():<7} ({elev_m}m): {n} days")
                except ERA5LagError as lag:
                    print(f"  [!] ERA5-Land lag: data only available until {lag.available_until}. Retrying…")
                    _discovered_end_date = lag.available_until
                    n = fetch_merged(session, name, lat, lon, elev_label,
                                     START_DATE, lag.available_until, elev_m)
                    print(f"  ✓ {elev_label.capitalize():<7} ({elev_m}m): {n} days  [capped at {lag.available_until}]")
            time.sleep(0.6)

    except Exception as e:
        print(f"\n[!] CRITICAL: Open-Meteo fetch failed: {e}")
        print("    Initiating Hard Fallback: Generating Synthetic Data…")
        synth_script = Path(__file__).parent / "generate_synthetic.py"
        result = subprocess.run([sys.executable, str(synth_script)])
        if result.returncode != 0:
            sys.exit(1)


if __name__ == "__main__":
    main()

