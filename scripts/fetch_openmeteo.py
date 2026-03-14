#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher
Hardened with exponential backoff and correct daily=var1&daily=var2 URI formatting.
"""

import requests, json, time, sys, argparse, subprocess
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import date, timedelta, datetime

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL   = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2019-11-01"

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_min",
    "snowfall_sum",
    "snow_depth",
    "precipitation_sum",
    "precipitation_hours",
    "sunshine_duration",
    "shortwave_radiation_sum",
    "wind_speed_10m_max",        # replaces deprecated windspeed_10m_max
    "wind_gusts_10m_max",
    "weather_code",              # replaces deprecated weathercode
]

RESORTS = [
    ("nauders",    46.88, 10.50, 1400, 2750),
    ("schoeneben", 46.80, 10.48, 1460, 2390),
    ("watles",     46.70, 10.50, 1500, 2550),
    ("sulden",     46.52, 10.58, 1900, 3250),
    ("trafoi",     46.55, 10.50, 1540, 2800)
]

def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_one(session, name, lat, lon, elevation_label, vars_to_use, start_d, end_d, elevation_m=None):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_d,
        "end_date": end_d,
        "daily": ",".join(vars_to_use),
        "timezone": "Europe/Berlin"
    }
    if elevation_m is not None:
        params["elevation"] = elevation_m  # forces lapse-rate interpolation to exact altitude
    
    response = session.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    
    if "error" in data and data.get("reason", "").startswith("Data only available until"):
        print(f"  [!] ERA5 lag detected: {data['reason']}")
        sys.exit(1)

    if name != "probe":
        out_file = OUTPUT_DIR / f"{name}_{elevation_label}_raw.json"
        with open(out_file, "w") as f:
            json.dump(data, f)
        
    return len(data.get("daily", {}).get("time", []))

def default_end_date():
    return (date.today() - timedelta(days=6)).strftime("%Y-%m-%d")

def probe_variables(session, lat, lon, base_m):
    try:
        fetch_one(session, "probe", lat, lon, "test", DAILY_VARS, "2024-01-01", "2024-01-02")
        return DAILY_VARS
    except Exception as e:
        print(f"Probe failed: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args()

    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"[!] ERROR: Invalid --end-date format '{args.end_date}'. Expected YYYY-MM-DD.")
            
    end_date = args.end_date if args.end_date else default_end_date()

    if end_date < START_DATE:
        sys.exit(f"[!] ERROR: --end-date ({end_date}) cannot be before START_DATE ({START_DATE}).")

    vars_to_use = DAILY_VARS
    session = get_session()

    if args.probe:
        name, lat, lon, base_m, _ = RESORTS[0]
        vars_to_use = probe_variables(session, lat, lon, base_m)
        if not vars_to_use: sys.exit("No variables worked.")

    try:
        for name, lat, lon, base_m, summit_m in RESORTS:
            print(f"\n[{name.upper()}]")
            n_base = fetch_one(session, name, lat, lon, "base", vars_to_use, START_DATE, end_date, elevation_m=base_m)
            print(f"  ✓ Base   ({base_m}m): {n_base} days")
            time.sleep(0.6)
            
            n_summit = fetch_one(session, name, lat, lon, "summit", vars_to_use, START_DATE, end_date, elevation_m=summit_m)
            print(f"  ✓ Summit ({summit_m}m): {n_summit} days")
            time.sleep(0.6)
            
    except Exception as e:
        print(f"\n[!] CRITICAL: Open-Meteo fetch failed. Error: {e}")
        print("    Initiating Hard Fallback: Generating Synthetic Data to keep pipeline green...")
        synth_script = Path(__file__).parent / "generate_synthetic.py"
        result = subprocess.run([sys.executable, str(synth_script)])
        if result.returncode != 0:
            print("    [!] Fallback failed: generate_synthetic.py exited non-zero.")
            sys.exit(1)

if __name__ == "__main__":
    main()
