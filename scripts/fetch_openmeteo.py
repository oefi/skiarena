#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher

Dual-call strategy per elevation:
  Call A — ERA5-Seamless (elevation + models=era5_seamless): all daily weather variables.
            Probe (2026-03-16) confirmed era5_seamless returns sunshine_duration natively
            and self-consistently — using it directly is more accurate than deriving via
            Angstrom-Prescott, which was calibrated for ERA5/best_match radiation schemes
            and produces ~67% error against era5_seamless shortwave values.
            Effective data lag: ~6 days (reduced from 7 under best_match; lag machinery kept).
  Call B — ERA5-Land (elevation + models=era5_land): snow_depth HOURLY.
            snow_depth has NO daily aggregate in the archive API — we request
            hourly and compute the daily MAX ourselves.
  Merge  — aggregate hourly snow_depth to daily MAX, inject into Call A daily dict.

Incremental / delta mode (default):
  On each run, inspects the last date already present in each cached raw JSON
  and fetches only the missing tail. Falls back to a full fetch from START_DATE
  when no cache exists or when --force is supplied. This reduces a typical
  scheduled run from ~2,300 API-days per file to the ~7-day ERA5-Land lag window.

ERA5-Land note: publishing lag is 5–7 days. ERA5LagError detects the
"Data only available until …" 400 response, retries with the real cutoff,
and caches it globally for subsequent resort fetches in the same run.

Atomic saves: all writes go through a .tmp sibling + rename so a mid-write
crash or kill cannot corrupt an existing cache file.
"""

import requests, json, time, sys, argparse, subprocess, re
from collections import defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import date, timedelta, datetime, timezone

OUTPUT_DIR    = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL      = "https://archive-api.open-meteo.com/v1/archive"
START_DATE    = "2019-11-01"
ERA5_LAG_DAYS = 6   # era5_seamless lag ~6 days (probed 2026-03-16)

# Call A — ERA5-Seamless (elevation param supplied). All variables including snow_depth.
# Probe confirmed era5_seamless returns sunshine_duration non-null and self-consistent.
ERA5_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_min",
    "apparent_temperature_max",
    "snowfall_sum",
    "precipitation_sum",
    "precipitation_hours",
    "sunshine_duration",       # native from era5_seamless — more accurate than A-P derivation
    "shortwave_radiation_sum",  # retained for diagnostics and record completeness
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "weather_code",
]

RESORTS = [
    ("nauders",    46.88, 10.50, 1400, 2750),
    ("schoeneben", 46.80, 10.48, 1460, 2390),
    ("watles",     46.70, 10.50, 1500, 2550),
    ("sulden",     46.52, 10.58, 1900, 3250),
    ("trafoi",     46.55, 10.50, 1540, 2800),
]

# Shared ERA5-Land cutoff discovered at runtime — avoids redundant 400 round-trips.
_discovered_end_date: str | None = None


# ── Session ───────────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ── Errors ────────────────────────────────────────────────────────────────────

class ERA5LagError(Exception):
    """Raised when ERA5-Land reports its data cutoff via a 400 response."""
    def __init__(self, available_until: str):
        self.available_until = available_until
        super().__init__(f"ERA5-Land data only available until {available_until}")


class AlreadyCurrentError(Exception):
    """Raised when the cache is already at or beyond the target end date."""
    pass


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(session: requests.Session, params: dict) -> dict:
    """
    GET with body-aware error handling.
    Handles both HTTP 400 and rare 200-with-error-body from Open-Meteo.
    Raises ERA5LagError, ValueError, or requests.HTTPError as appropriate.
    """
    resp = session.get(BASE_URL, params=params, timeout=15)
    try:
        body = resp.json()
    except Exception:
        body = {}

    # Parse lag cutoff from 400 body before raising
    if resp.status_code == 400:
        reason = body.get("reason", "")
        if "only available until" in reason:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", reason)
            if m:
                raise ERA5LagError(m.group(1))
        resp.raise_for_status()

    resp.raise_for_status()

    # Open-Meteo occasionally returns 200 with an error payload
    if body.get("error") and "only available until" in body.get("reason", ""):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", body["reason"])
        if m:
            raise ERA5LagError(m.group(1))

    n_daily  = len(body.get("daily",  {}).get("time", []))
    n_hourly = len(body.get("hourly", {}).get("time", []))
    if n_daily == 0 and n_hourly == 0:
        raise ValueError(f"API returned 200 but zero records for params: {params}")

    return body


# ── Cache helpers ─────────────────────────────────────────────────────────────

def get_cached_end_date(name: str, elevation_label: str) -> str | None:
    """
    Return the last date string present in the cached raw JSON file, or None.
    Returns None (triggering a full fetch) on: missing file, empty time array,
    JSON decode error, or any other read failure. Corruption → safe full re-fetch.
    """
    path = OUTPUT_DIR / f"{name}_{elevation_label}_raw.json"
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        times = data.get("daily", {}).get("time", [])
        return max(times) if times else None
    except Exception:
        return None


def _atomic_save(data: dict, path: Path) -> None:
    """
    Write JSON to a .tmp sibling then atomically rename into place.
    A crash or SIGKILL between the two operations leaves the original
    untouched; the orphaned .tmp is harmless and will be overwritten next run.
    """
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(data, f)
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ── Delta merge ───────────────────────────────────────────────────────────────

def merge_daily(existing: dict, delta: dict) -> dict:
    """
    Append delta's daily arrays onto existing. Returns a merged dict.

    Deduplication  — existing wins on overlapping dates. Delta dates already
                     present in existing are silently dropped. This means a safe
                     overlap on the delta start date is fine (and expected when
                     ERA5-Land lag shifts between runs).

    Schema gaps    — variables in existing but absent from delta are extended
                     with None for the new rows. Variables in delta but absent
                     from existing are backfilled with None for all prior rows,
                     then appended normally. This handles API schema additions
                     without corrupting array lengths.

    Units          — delta wins for any key present in both (may add new keys).
    """
    ex_times = existing["daily"].get("time", [])
    dl_times = delta["daily"].get("time", [])

    ex_set      = set(ex_times)
    new_indices = [i for i, t in enumerate(dl_times) if t not in ex_set]

    if not new_indices:
        return existing  # already current; caller may log this

    n_existing = len(ex_times)

    # Build merged dict preserving all top-level keys except "daily"
    merged       = {k: v for k, v in existing.items() if k != "daily"}
    merged_daily = {}

    all_keys = (set(existing["daily"].keys()) | set(delta["daily"].keys())) - {"time"}

    merged_daily["time"] = ex_times + [dl_times[i] for i in new_indices]

    for key in all_keys:
        ex_arr = list(existing["daily"].get(key) or [])
        dl_arr = list(delta["daily"].get(key) or [])

        # Backfill if this key is new (not in existing): pad prior rows with None
        if len(ex_arr) < n_existing:
            ex_arr = ex_arr + [None] * (n_existing - len(ex_arr))

        new_vals = [dl_arr[i] if i < len(dl_arr) else None for i in new_indices]
        merged_daily[key] = ex_arr + new_vals

    merged["daily"] = merged_daily

    # Merge units: delta wins (may introduce new keys)
    if "daily_units" in delta:
        merged["daily_units"] = {
            **existing.get("daily_units", {}),
            **delta.get("daily_units", {}),
        }

    return merged


# ── Core fetch ────────────────────────────────────────────────────────────────

def fetch_merged(
    session: requests.Session,
    name: str,
    lat: float,
    lon: float,
    elevation_label: str,
    start_d: str,
    end_d: str,
    elevation_m: int,
) -> tuple[dict, int]:
    """
    Perform Call A (ERA5 best-match, elevation-corrected) + Call B (ERA5-Land snow_depth
    hourly), merge snow_depth into ERA5 daily dict. Returns (data_dict, n_days_fetched).
    Does NOT write to disk — the caller owns save / merge decisions.
    """
    base_params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_d,
        "end_date":   end_d,
        "daily":      ",".join(ERA5_VARS),
        "timezone":   "Europe/Berlin",
        "elevation":  elevation_m,   # triggers lapse-rate downscaling for temp/wind
        "models":     "era5_seamless", # blends ERA5-Land + IFS; native sunshine_duration; ~6d lag
    }
    depth_params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_d,
        "end_date":   end_d,
        "hourly":     "snow_depth",   # snow_depth is HOURLY-ONLY in the archive API
        "timezone":   "Europe/Berlin",
        "elevation":  elevation_m,
        "models":     "era5_land",    # snow_depth only exists in ERA5-Land
    }

    era5_data  = _get(session, base_params)
    time.sleep(2.0)   # pace between Call A and Call B — Open-Meteo free tier ~10 req/min
    depth_data = _get(session, depth_params)

    # Aggregate hourly snow_depth → daily MAX and inject into ERA5 daily dict.
    # max() is ski-conservative: never understates the snow available at day's end.
    # A melt day's hourly depths decline through the afternoon; the mean would
    # overstate usable snow. A snowfall day accumulates through the night; the
    # max captures what's there when the first chair opens the next morning.
    #
    # DST NOTE: timestamps are in Europe/Berlin local time (timezone param above).
    # ts[:10] correctly extracts the Alpine local date for bucketing. On the
    # DST spring-forward day (~last Sunday of March) only 23 hourly values exist
    # for that date — the 02:xx hour is skipped by the OS. This is benign here
    # because we take MAX across available readings; one fewer reading does not
    # materially change the daily peak depth.
    era5_dates   = era5_data["daily"].get("time", [])
    hourly_times = depth_data["hourly"].get("time", [])
    hourly_depth = depth_data["hourly"].get("snow_depth", [])

    daily_depth_raw: dict = defaultdict(list)
    for ts, v in zip(hourly_times, hourly_depth):
        if v is not None:
            daily_depth_raw[ts[:10]].append(v)

    depth_by_date = {
        d: max(vs) for d, vs in daily_depth_raw.items() if vs
    }

    if len(era5_dates) != len(depth_by_date):
        print(
            f"  [!] WARNING: ERA5 ({len(era5_dates)} days) and ERA5-Land "
            f"({len(depth_by_date)} days aggregated) date ranges differ for "
            f"{name}/{elevation_label}. snow_depth will be None for gaps."
        )

    era5_data["daily"]["snow_depth"] = [
        depth_by_date.get(d) for d in era5_dates
    ]
    if "daily_units" in era5_data and "hourly_units" in depth_data:
        era5_data["daily_units"]["snow_depth"] = (
            depth_data["hourly_units"].get("snow_depth", "m")
        )

    return era5_data, len(era5_dates)


# ── Lag-aware fetch wrapper ───────────────────────────────────────────────────

def _fetch_with_lag_retry(
    session: requests.Session,
    name: str,
    lat: float,
    lon: float,
    elev_label: str,
    fetch_start: str,
    end_d: str,
    elev_m: int,
) -> tuple[dict, int, str]:
    """
    Wrap fetch_merged with ERA5LagError detection and single retry.
    Updates the module-level _discovered_end_date on lag discovery so all
    subsequent resort fetches in the same run skip the redundant 400.

    Returns (data, n_days_fetched, effective_end_date_used).
    Raises AlreadyCurrentError if the lag cutoff is before fetch_start
    (meaning the cache is already fully current up to what ERA5-Land has).
    """
    global _discovered_end_date

    try:
        data, n = fetch_merged(session, name, lat, lon, elev_label,
                               fetch_start, end_d, elev_m)
        return data, n, end_d

    except ERA5LagError as lag:
        print(f"  [!] ERA5-Land lag: available until {lag.available_until}. Retrying…")
        _discovered_end_date = lag.available_until

        if lag.available_until < fetch_start:
            raise AlreadyCurrentError(
                f"cache already current through {fetch_start}; "
                f"ERA5-Land cutoff is {lag.available_until}"
            )

        data, n = fetch_merged(session, name, lat, lon, elev_label,
                               fetch_start, lag.available_until, elev_m)
        return data, n, lag.available_until


# ── Helpers ───────────────────────────────────────────────────────────────────

def default_end_date() -> str:
    # Use UTC explicitly. GitHub Actions runners are UTC; date.today() would also
    # return UTC there, but is ambiguous on local machines near midnight.
    # ERA5 archive timestamps are UTC-anchored, so UTC is the correct reference.
    # Alpine local time (CET/CEST) is UTC+1/+2 — using UTC avoids a 1–2 hour
    # window around midnight where date.today() returns yesterday's Alpine date,
    # which would cause the lag window to be calculated from the wrong base.
    return (datetime.now(timezone.utc).date() - timedelta(days=ERA5_LAG_DAYS)).strftime("%Y-%m-%d")


def _next_day(date_str: str) -> str:
    return (
        datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global _discovered_end_date

    parser = argparse.ArgumentParser(description="Fetch Open-Meteo historical data")
    parser.add_argument("--end-date", default=None,
                        help="Override fetch end date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true",
                        help="Ignore existing cache; full re-fetch from START_DATE")
    parser.add_argument("--probe", action="store_true",
                        help="Test API connectivity with a minimal 2-day fetch and exit")
    args = parser.parse_args()

    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"[!] Invalid --end-date '{args.end_date}'. Expected YYYY-MM-DD.")

    end_date = args.end_date or default_end_date()
    if end_date < START_DATE:
        sys.exit(f"[!] --end-date ({end_date}) is before START_DATE ({START_DATE}).")

    session = get_session()

    # ── Probe mode ────────────────────────────────────────────────────────────
    if args.probe:
        try:
            fetch_merged(session, "probe", RESORTS[0][1], RESORTS[0][2],
                         "probe", "2024-01-01", "2024-01-02", RESORTS[0][3])
            print("Probe OK — all variables accessible.")
        except Exception as e:
            sys.exit(f"Probe failed: {e}")
        return

    # ── Main fetch loop ───────────────────────────────────────────────────────
    try:
        for name, lat, lon, base_m, summit_m in RESORTS:
            print(f"\n[{name.upper()}]")

            for elev_label, elev_m in [("base", base_m), ("summit", summit_m)]:
                effective_end = _discovered_end_date or end_date
                out_file      = OUTPUT_DIR / f"{name}_{elev_label}_raw.json"

                # ── Delta detection ───────────────────────────────────────────
                cached_end  = None if args.force else get_cached_end_date(name, elev_label)
                fetch_start = START_DATE

                if cached_end:
                    fetch_start = _next_day(cached_end)

                    if fetch_start > effective_end:
                        print(f"  ✓ {elev_label.capitalize():<7} ({elev_m}m): "
                              f"already current through {cached_end}")
                        continue

                    print(f"  → {elev_label.capitalize():<7} ({elev_m}m): "
                          f"cached through {cached_end}, "
                          f"delta {fetch_start} → {effective_end}")
                else:
                    mode = "--force: full re-fetch" if args.force else "no cache, full fetch"
                    print(f"  → {elev_label.capitalize():<7} ({elev_m}m): "
                          f"{mode} from {START_DATE}")

                # ── Fetch (with lag retry) ────────────────────────────────────
                try:
                    data, n_fetched, used_end = _fetch_with_lag_retry(
                        session, name, lat, lon, elev_label,
                        fetch_start, effective_end, elev_m,
                    )
                except AlreadyCurrentError as e:
                    print(f"  ✓ {elev_label.capitalize():<7} ({elev_m}m): {e}")
                    continue

                # ── Merge delta into existing (skip on full fetch / --force) ──
                if cached_end and out_file.exists():
                    try:
                        with open(out_file) as f:
                            existing = json.load(f)
                        merged = merge_daily(existing, data)
                        if merged is existing:
                            # merge_daily returned the original — delta was pure overlap
                            print(f"  ✓ {elev_label.capitalize():<7} ({elev_m}m): "
                                  f"delta contained no new dates (overlap only)")
                            continue
                        data = merged
                    except json.JSONDecodeError as e:
                        # Cache is corrupt — write delta as-is; back up the corrupt file.
                        bak = out_file.with_suffix(".bak")
                        print(f"  [!] WARNING: {out_file.name} is corrupt ({e}). "
                              f"Writing delta only; corrupt file saved to .bak")
                        try:
                            out_file.replace(bak)
                        except Exception:
                            pass  # best-effort; atomic save below will still succeed
                    except Exception as e:
                        # Unexpected merge failure — keep existing file intact,
                        # skip this elevation rather than overwriting with partial data.
                        print(f"  [!] ERROR: merge failed for {name}/{elev_label}: {e}")
                        print(f"      Existing file untouched. Will retry next run.")
                        continue

                # ── Atomic save ───────────────────────────────────────────────
                _atomic_save(data, out_file)

                total_days = len(data.get("daily", {}).get("time", []))
                lag_note   = f"  [capped at {used_end}]" if used_end != effective_end else ""
                print(f"  ✓ {elev_label.capitalize():<7} ({elev_m}m): "
                      f"+{n_fetched} new days, {total_days} total{lag_note}")

            time.sleep(3.0)   # between resorts — keeps full force-rebuild within rate limits

    except Exception as e:
        print(f"\n[!] CRITICAL: Open-Meteo fetch failed: {e}")
        print("    Initiating Hard Fallback: Generating Synthetic Data…")
        synth_script = Path(__file__).parent / "generate_synthetic.py"
        result = subprocess.run([sys.executable, str(synth_script)])
        if result.returncode != 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
