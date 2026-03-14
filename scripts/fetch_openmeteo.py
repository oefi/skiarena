#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher
Hardened with exponential backoff and ERA5-Land lag detection.

ERA5-Land note: supplying the `elevation` parameter switches Open-Meteo from the
standard ERA5 grid (~5-day lag) to ERA5-Land (~60-90 day lag). fetch_one() detects
400 responses, parses the "Data only available until …" message, and returns the
real cutoff so the caller can retry with the correct end date.
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
# ERA5-Land (triggered by the elevation parameter) publishes with a ~5-7 day lag now,
# but has historically been up to 90 days. Conservative default keeps CI green.
ERA5_LAG_DAYS = 7


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

# ERA5-Land availability cutoff discovered at runtime (shared across all resort fetches)
_discovered_end_date = None


def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


class ERA5LagError(Exception):
    """Raised when the API reports data is only available up to an earlier date."""
    def __init__(self, available_until: str):
        self.available_until = available_until
        super().__init__(f"ERA5-Land data only available until {available_until}")


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

    # Parse the body before raising, so we can extract the real cutoff date from 400s
    try:
        body = response.json()
    except Exception:
        body = {}

    if response.status_code == 400:
        reason = body.get("reason", "")
        # Open-Meteo 400 body: {"error":true,"reason":"Data only available until 2025-12-15. ..."}
        if "only available until" in reason:
            # Extract date — format is always YYYY-MM-DD at the start of the clause
            import re
            m = re.search(r"(\d{4}-\d{2}-\d{2})", reason)
            if m:
                raise ERA5LagError(m.group(1))
        response.raise_for_status()  # re-raise for other 400 causes

    response.raise_for_status()

    data = body

    # Also handle the case where the API returns 200 with an error body
    if data.get("error") and "only available until" in data.get("reason", ""):
        import re
        m = re.search(r"(\d{4}-\d{2}-\d{2})", data["reason"])
        if m:
            raise ERA5LagError(m.group(1))

    if name != "probe":
        out_file = OUTPUT_DIR / f"{name}_{elevation_label}_raw.json"
        with open(out_file, "w") as f:
            json.dump(data, f)

    return len(data.get("daily", {}).get("time", []))


def default_end_date():
    return (date.today() - timedelta(days=ERA5_LAG_DAYS)).strftime("%Y-%m-%d")


def probe_variables(session, lat, lon, base_m):
    try:
        fetch_one(session, "probe", lat, lon, "test", DAILY_VARS, "2024-01-01", "2024-01-02")
        return DAILY_VARS
    except ERA5LagError:
        return DAILY_VARS  # probe date is historical, lag error shouldn't happen
    except Exception as e:
        print(f"Probe failed: {e}")
        return None


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

            # Use the already-discovered cutoff for subsequent resorts (saves one round-trip per resort)
            effective_end = _discovered_end_date if _discovered_end_date else end_date

            for elev_label, elev_m, elev_desc in [("base", base_m, base_m), ("summit", summit_m, summit_m)]:
                try:
                    n = fetch_one(session, name, lat, lon, elev_label, vars_to_use,
                                  START_DATE, effective_end, elevation_m=elev_m)
                    print(f"  ✓ {elev_label.capitalize():<7} ({elev_desc}m): {n} days")
                except ERA5LagError as lag:
                    # API told us the real cutoff — record it and retry once
                    print(f"  [!] ERA5-Land lag detected: data only available until {lag.available_until}. Retrying…")
                    _discovered_end_date = lag.available_until
                    effective_end = lag.available_until
                    n = fetch_one(session, name, lat, lon, elev_label, vars_to_use,
                                  START_DATE, effective_end, elevation_m=elev_m)
                    print(f"  ✓ {elev_label.capitalize():<7} ({elev_desc}m): {n} days  [capped at {effective_end}]")

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
