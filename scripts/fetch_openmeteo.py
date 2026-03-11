#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher

Requirements: pip install requests urllib3
Run:          python fetch_openmeteo.py [--end-date YYYY-MM-DD] [--probe]
"""

import requests, json, time, sys, argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import date, timedelta, datetime

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL   = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2019-12-10"

DAILY_VARS = [
    "temperature_2m_max", "temperature_2m_min", "snowfall_sum", 
    "snow_depth", "precipitation_sum", "shortwave_radiation_sum", 
    "windspeed_10m_max", "weathercode"
]

RESORTS = [
    ("nauders",    46.88, 10.50, 1400, 2750),
    ("schoeneben", 46.80, 10.48, 1460, 2390),
    ("watles",     46.70, 10.50, 1500, 2550),
    ("sulden",     46.52, 10.58, 1900, 3250),
    ("trafoi",     46.55, 10.50, 1540, 2800)
]

def get_session():
    """Builds a hardened requests session with exponential backoff for the Open-Meteo API."""
    session = requests.Session()
    # Retries on Rate Limit (429) and Server Errors (5xx)
    # Sleeps: 1s, 2s, 4s, 8s, 16s before giving up
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_one(session, name, lat, lon, elevation_label, vars_to_use, start_d, end_d):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_d,
        "end_date": end_d,
        "daily": vars_to_use,
        "timezone": "Europe/Berlin"
    }
    
    try:
        response = session.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Catch 200 OKs that carry API error payloads
        if "error" in data and data.get("reason", "").startswith("Data only available until"):
            print(f"  [!] ERA5 lag detected: {data['reason']}")
            print("      Run orchestrator with an earlier --end-date.")
            sys.exit(1)

        # Skip polluting the raw folder if this is just a probe
        if name != "probe":
            out_file = OUTPUT_DIR / f"{name}_{elevation_label}_raw.json"
            with open(out_file, "w") as f:
                json.dump(data, f)
            
        return len(data.get("daily", {}).get("time", []))
        
    except requests.exceptions.HTTPError as e:
        # Gracefully handle 400 Bad Requests specific to ERA5 future-date limitations
        if e.response is not None and e.response.status_code == 400:
            try:
                err_data = e.response.json()
                if err_data.get("error") and "Data only available until" in err_data.get("reason", ""):
                    print(f"  [!] ERA5 lag detected: {err_data['reason']}")
                    print(f"      Run orchestrator with an earlier --end-date.")
                    sys.exit(1)
            except ValueError:
                pass
        print(f"  [!] HTTP failure fetching {name} ({elevation_label}): {e}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"  [!] Network failure fetching {name} ({elevation_label}): {e}")
        raise

def default_end_date():
    """ERA5-Land is usually 5-6 days behind. Safely target 6 days ago."""
    return (date.today() - timedelta(days=6)).strftime("%Y-%m-%d")

def probe_variables(session, lat, lon, base_m):
    """Simple connection test."""
    try:
        fetch_one(session, "probe", lat, lon, "test", DAILY_VARS, "2024-01-01", "2024-01-02")
        return DAILY_VARS
    except Exception as e:
        print(f"Probe failed: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD override")
    parser.add_argument("--probe", action="store_true", help="Run connection probe before full fetch")
    args = parser.parse_args()

    # 1. User Input Validation: Strict Date Formatting
    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"[!] ERROR: Invalid --end-date format '{args.end_date}'. Expected YYYY-MM-DD.")
            
    end_date = args.end_date if args.end_date else default_end_date()

    # 2. User Input Validation: Chronological integrity
    if end_date < START_DATE:
        sys.exit(f"[!] ERROR: --end-date ({end_date}) cannot be before START_DATE ({START_DATE}).")

    vars_to_use = DAILY_VARS

    print("=" * 60)
    print("Zwei Länder Skiarena — Open-Meteo Data Fetch (Hardened)")
    print(f"Period:  {START_DATE} → {end_date}")
    print(f"Resorts: {len(RESORTS)} × 2 = {len(RESORTS)*2} calls")
    print(f"Vars ({len(vars_to_use)}): {', '.join(vars_to_use)}")
    print("=" * 60)

    session = get_session()

    if args.probe:
        name, lat, lon, base_m, _ = RESORTS[0]
        print(f"\n[PROBE on {name} base {base_m}m]")
        vars_to_use = probe_variables(session, lat, lon, base_m)
        if not vars_to_use:
            sys.exit("No variables worked — check network/API status.")
        print(f"Proceeding with {len(vars_to_use)} confirmed variables.")

    for name, lat, lon, base_m, summit_m in RESORTS:
        print(f"\n[{name.upper()}]")
        
        n_base = fetch_one(session, name, lat, lon, "base", vars_to_use, START_DATE, end_date)
        print(f"  ✓ Base   ({base_m}m): {n_base} days")
        time.sleep(0.6)  # Be gentle to the free API
        
        n_summit = fetch_one(session, name, lat, lon, "summit", vars_to_use, START_DATE, end_date)
        print(f"  ✓ Summit ({summit_m}m): {n_summit} days")
        time.sleep(0.6)

if __name__ == "__main__":
    main()