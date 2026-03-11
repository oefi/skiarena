#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher
Fetches daily weather for all 5 resorts × 2 elevations.
Season range: Dec 10 → Apr 10, seasons 2019/20 – 2024/25.
Season 2025/26 is handled by the live browser fetch layer.

Requirements: pip install requests
Run:          python fetch_openmeteo.py
Output:       ../data/raw/{resort}_{base|summit}_raw.json  (10 files)
"""

import requests
import json
import time
import sys
import argparse
from pathlib import Path
from datetime import date

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

START_DATE = "2019-12-10"   # first day of first baked season — never changes

def default_end_date():
    """
    Dynamic end date:
    - If today is in-season (Dec 10 ≤ date ≤ Apr 10): use yesterday
      (ERA5-Land typically has ~5-day lag; yesterday is safe)
    - If off-season: use the last Apr 10 that has passed
    """
    today = date.today()
    m, d  = today.month, today.day
    in_season = (m == 12 and d >= 10) or (1 <= m <= 3) or (m == 4 and d <= 10)
    if in_season:
        from datetime import timedelta
        return str(today - timedelta(days=2))  # 2-day ERA5 lag buffer
    # Off-season: last completed Apr 10
    last_apr10_year = today.year if today > date(today.year, 4, 10) else today.year - 1
    return f"{last_apr10_year}-04-10"

def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch Open-Meteo ERA5-Land data for Zwei Länder Skiarena")
    parser.add_argument(
        "--end-date", default=None,
        help="Last date to fetch (ISO 8601). Defaults to dynamic season-aware value.")
    return parser.parse_args()

args     = parse_args()
END_DATE = args.end_date or default_end_date()

DAILY_VARS = ",".join([
    "temperature_2m_max",
    "temperature_2m_min",
    "snowfall_sum",           # cm
    "snow_depth",             # m  → we convert to cm
    "precipitation_sum",      # mm
    "rain_sum",               # mm
    "sunshine_duration",      # seconds → we convert to hours
    "windspeed_10m_max",      # km/h
    "weathercode",            # WMO weather interpretation code
    # uv_index_max removed — not available in ERA5-Land archive API (forecast-only)
])

# ── Resorts: (name, lat, lon, base_m, summit_m) ───────────────────────────────

RESORTS = [
    ("nauders",    46.8897, 10.5042, 1400, 2750),
    ("schoeneben", 46.8067, 10.5233, 1460, 2390),
    ("watles",     46.6869, 10.5517, 1500, 2550),
    ("sulden",     46.5260, 10.5752, 1900, 3250),
    ("trafoi",     46.5581, 10.6008, 1540, 2800),
]

# ── Fetch function ─────────────────────────────────────────────────────────────

def fetch(resort_name, lat, lon, elevation, label):
    """One API call for a single resort at a single elevation."""
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "elevation":   elevation,
        "start_date":  START_DATE,
        "end_date":    END_DATE,
        "daily":       DAILY_VARS,
        "timezone":    "UTC",
        "wind_speed_unit": "kmh",
    }
    print(f"  Fetching {resort_name} ({label}, {elevation} m) ...", end=" ", flush=True)
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    # Validate
    n_days = len(data["daily"]["time"])
    print(f"{n_days} days OK")

    # Save raw
    out_path = OUTPUT_DIR / f"{resort_name}_{label}_raw.json"
    with open(out_path, "w") as f:
        json.dump(data, f)
    print(f"    → saved {out_path}")
    return n_days

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Zwei Länder Skiarena — Open-Meteo Data Fetch")
    print(f"Period: {START_DATE} → {END_DATE}")
    print(f"Resorts: {len(RESORTS)} × 2 elevations = {len(RESORTS)*2} API calls")
    print("=" * 60)

    summary = []
    for name, lat, lon, base_m, summit_m in RESORTS:
        print(f"\n[{name.upper()}]")

        # Base elevation
        n = fetch(name, lat, lon, base_m, "base")
        summary.append((name, "base", base_m, n))
        time.sleep(0.5)  # be polite to the free API

        # Summit elevation
        n = fetch(name, lat, lon, summit_m, "summit")
        summary.append((name, "summit", summit_m, n))
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Resort':<12} {'Elev':<8} {'Elevation m':>12} {'Days':>6}")
    print("-" * 42)
    for name, label, elev, days in summary:
        print(f"{name:<12} {label:<8} {elev:>12} {days:>6}")
    print("=" * 60)
    print(f"\nAll {len(summary)} files saved to {OUTPUT_DIR}")
    print("Next step: run scripts/clean_normalize.py")

if __name__ == "__main__":
    main()
