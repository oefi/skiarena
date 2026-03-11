#!/usr/bin/env python3
"""
Step 2 — Clean, Normalize & Merge
Reads all 10 raw JSON files, merges base + summit per resort per day,
normalizes units, validates continuity, outputs master_data.json.

Output:
  ../data/processed/master_data.json
  ../data/processed/data_quality_report.txt
"""

import json
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict

RAW_DIR  = Path(__file__).parent.parent / "data" / "raw"
OUT_DIR  = Path(__file__).parent.parent / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESORTS = ["nauders", "schoeneben", "watles", "sulden", "trafoi"]

RESORT_META = {
    "nauders":    {"label": "Nauders",           "country": "AT", "base_m": 1400, "summit_m": 2750, "ski6": False},
    "schoeneben": {"label": "Schöneben–Haideralm","country": "IT", "base_m": 1460, "summit_m": 2390, "ski6": False},
    "watles":     {"label": "Watles",             "country": "IT", "base_m": 1500, "summit_m": 2550, "ski6": False},
    "sulden":     {"label": "Sulden am Ortler",   "country": "IT", "base_m": 1900, "summit_m": 3250, "ski6": True},
    "trafoi":     {"label": "Trafoi am Ortler",   "country": "IT", "base_m": 1540, "summit_m": 2800, "ski6": True},
}

# ── Dynamic SEASONS — derived from the raw data files actually present ────────
# This means adding a new season only requires a new raw fetch, not code changes.

def _raw_date_range():
    """Scan raw JSON files to find the earliest and latest dates present."""
    earliest, latest = None, None
    for f in RAW_DIR.glob("*_raw.json"):
        try:
            times = json.loads(f.read_text())["daily"]["time"]
            if times:
                if earliest is None or times[0]  < earliest: earliest = times[0]
                if latest   is None or times[-1] > latest:   latest   = times[-1]
        except Exception:
            pass
    return earliest, latest

def _build_seasons(earliest_str, latest_str):
    """
    Build (key, start, end) tuples for every Dec 10 → Apr 10 window
    that overlaps the [earliest_str, latest_str] date range.
    A partial current season (e.g. Dec 10 2025 → Mar 11 2026) is included
    with its actual end date rather than Apr 10.
    """
    from datetime import date, timedelta
    if not earliest_str or not latest_str:
        return []

    first = date.fromisoformat(earliest_str)
    last  = date.fromisoformat(latest_str)

    # First season start year: Dec 10 of the year first falls in
    start_yr = first.year if first.month == 12 else first.year - 1
    # Last season start year
    end_yr   = last.year if last.month == 12 else last.year - 1

    seasons = []
    for y in range(start_yr, end_yr + 1):
        s_start = date(y,     12, 10)
        s_end   = date(y + 1,  4, 10)
        key     = f"{y}/{str(y + 1)[2:]}"
        # Clip end to actual data end (for in-progress season)
        actual_end = min(s_end, last)
        if actual_end >= s_start:
            seasons.append((key, s_start.isoformat(), actual_end.isoformat()))
    return seasons

_earliest, _latest = _raw_date_range()
SEASONS = _build_seasons(_earliest, _latest) if _earliest else [
    # Fallback for dev without raw files (matches synthetic data)
    ("2019/20", "2019-12-10", "2020-04-10"),
    ("2020/21", "2020-12-10", "2021-04-10"),
    ("2021/22", "2021-12-10", "2022-04-10"),
    ("2022/23", "2022-12-10", "2023-04-10"),
    ("2023/24", "2023-12-10", "2024-04-10"),
    ("2024/25", "2024-12-10", "2025-04-10"),
]

def expected_dates():
    dates = []
    for _, start, end in SEASONS:
        d   = date.fromisoformat(start)
        end = date.fromisoformat(end)
        while d <= end:
            dates.append(d.isoformat())
            d += timedelta(days=1)
    return dates

EXPECTED     = expected_dates()
EXPECTED_SET = set(EXPECTED)

# ── Season key helpers ────────────────────────────────────────────────────────

def get_season(date_str):
    """'2024-12-25' → '2024/25',  '2025-02-10' → '2024/25'"""
    month = int(date_str[5:7])
    year  = int(date_str[:4])
    s     = year if month == 12 else year - 1
    return f"{s}/{str(s + 1)[2:]}"

def get_season_week(date_str):
    """Season-relative week: W01 = week containing Dec 10."""
    from datetime import date
    d     = date.fromisoformat(date_str)
    month = d.month
    dec10 = date(d.year if month == 12 else d.year - 1, 12, 10)
    return f"W{(d - dec10).days // 7 + 1:02d}"

# ── Load one raw file ─────────────────────────────────────────────────────────

def load_raw(resort, label):
    p = RAW_DIR / f"{resort}_{label}_raw.json"
    with open(p) as f:
        return json.load(f)

# ── Forward-fill single-day nulls ─────────────────────────────────────────────

def forward_fill(values):
    filled = 0
    for i in range(1, len(values)):
        if values[i] is None and values[i-1] is not None:
            values[i] = values[i-1]
            filled += 1
    return values, filled

# ── Merge base + summit into one record per day ───────────────────────────────

def merge_resort(resort):
    base_raw   = load_raw(resort, "base")
    summit_raw = load_raw(resort, "summit")

    bd = base_raw["daily"]
    sd = summit_raw["daily"]

    # Index summit by date for O(1) lookup
    summit_idx = {t: i for i, t in enumerate(sd["time"])}

    records = []
    issues  = []

    for i, dt in enumerate(bd["time"]):
        if dt not in EXPECTED_SET:
            continue  # skip dates outside Dec 10 → Apr 10 window

        # ── Base values (unit conversions) ──
        base_temp_max  = bd["temperature_2m_max"][i]
        base_temp_min  = bd["temperature_2m_min"][i]
        fresh_snow_cm  = bd["snowfall_sum"][i]                   # already cm
        base_depth_cm  = round((bd["snow_depth"][i] or 0) * 100, 1)   # m → cm
        precip_mm      = bd["precipitation_sum"][i] or 0
        rain_mm        = max(0.0, round(precip_mm - snowfall_cm * 0.7, 2))  # derived: precip − snow water equiv
        # shortwave_radiation_sum (MJ/m²) → sunshine hours equivalent
        # ~25 MJ/m² = full alpine sunny day ≈ 8h; divide by 3.1
        rad_mj         = bd.get("shortwave_radiation_sum", [None] * (i+1))[i] or 0
        sunshine_h     = round(min(rad_mj / 3.1, 16.0), 2)
        wind_kmh       = bd["windspeed_10m_max"][i] or 0
        weathercode    = bd["weathercode"][i] or 0
        uv_max         = (bd.get("uv_index_max") or [0])[i] or 0

        # ── Summit values ──
        if dt in summit_idx:
            si = summit_idx[dt]
            summit_temp_max = sd["temperature_2m_max"][si]
            summit_temp_min = sd["temperature_2m_min"][si]
            summit_depth_cm = round((sd["snow_depth"][si] or 0) * 100, 1)
            summit_wind_kmh = sd["windspeed_10m_max"][si] or 0
        else:
            # Fallback: lapse-rate approximation (-0.6°C per 100m)
            meta  = RESORT_META[resort]
            delta = (meta["summit_m"] - meta["base_m"]) / 100 * 0.6
            summit_temp_max = round(base_temp_max - delta, 1) if base_temp_max else None
            summit_temp_min = round(base_temp_min - delta, 1) if base_temp_min else None
            summit_depth_cm = round(base_depth_cm * 1.4, 1)   # rough estimate
            summit_wind_kmh = round(wind_kmh * 1.3, 1)
            issues.append(f"{dt}: summit data missing, lapse-rate fallback used")

        # ── Null checks ──
        for fname, val in [("base_temp_max", base_temp_max),
                           ("base_depth_cm", base_depth_cm),
                           ("sunshine_h",    sunshine_h)]:
            if val is None:
                issues.append(f"{dt}: null in {fname}")

        # ── Precipitation type classification ──
        if rain_mm > 1 and base_temp_max > 0:
            precip_type = "rain"
        elif fresh_snow_cm > 0:
            precip_type = "snow"
        elif precip_mm > 0:
            precip_type = "mixed"
        else:
            precip_type = "none"

        # ── Freezing level (m) — simple linear lapse-rate estimate ──
        meta = RESORT_META[resort]
        if base_temp_max and base_temp_max > 0:
            # 0°C isotherm above base: ~100m per 0.6°C
            freezing_level_m = round(meta["base_m"] + (base_temp_max / 0.6) * 100)
        else:
            freezing_level_m = meta["base_m"]  # at or below base

        records.append({
            "date":              dt,
            "season":            get_season(dt),
            "season_week":       get_season_week(dt),
            "year":              int(dt[:4]),
            "month":             int(dt[5:7]),
            "day":               int(dt[8:10]),
            "resort":            resort,
            # Base
            "base_temp_max":     round(base_temp_max, 1) if base_temp_max is not None else None,
            "base_temp_min":     round(base_temp_min, 1) if base_temp_min is not None else None,
            "base_depth_cm":     base_depth_cm,
            "fresh_snow_cm":     round(fresh_snow_cm, 1) if fresh_snow_cm else 0.0,
            "rain_mm":           round(rain_mm, 1),
            "precip_mm":         round(precip_mm, 1),
            "precip_type":       precip_type,
            "sunshine_h":        sunshine_h,
            "wind_kmh":          round(wind_kmh, 1),
            "weathercode":       weathercode,
            "uv_max":            uv_max,
            "freezing_level_m":  freezing_level_m,
            # Summit
            "summit_temp_max":   round(summit_temp_max, 1) if summit_temp_max is not None else None,
            "summit_temp_min":   round(summit_temp_min, 1) if summit_temp_min is not None else None,
            "summit_depth_cm":   summit_depth_cm,
            "summit_wind_kmh":   round(summit_wind_kmh, 1),
        })

    return records, issues

# ── Validate date continuity ───────────────────────────────────────────────────

def validate_continuity(records, resort):
    found = {r["date"] for r in records}
    missing = [d for d in EXPECTED if d not in found]
    return missing

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("Step 2 — Clean, Normalize & Merge")
    print("=" * 65)

    all_records = []
    quality_lines = []

    for resort in RESORTS:
        records, issues = merge_resort(resort)
        missing = validate_continuity(records, resort)

        n = len(records)
        status = "✓ PASS" if not missing and not issues else "⚠ WARN"
        print(f"  {resort:<12}  {n:>4} records  {len(issues):>3} issues  {len(missing):>3} missing dates  {status}")

        quality_lines.append(f"\n{'='*65}")
        quality_lines.append(f"Resort: {resort.upper()}  ({n} records)")
        quality_lines.append(f"  Issues : {len(issues)}")
        quality_lines.append(f"  Missing dates: {len(missing)}")
        if issues:
            for iss in issues[:10]:
                quality_lines.append(f"    ! {iss}")
            if len(issues) > 10:
                quality_lines.append(f"    ... and {len(issues)-10} more")
        if missing:
            for m in missing[:5]:
                quality_lines.append(f"    missing: {m}")

        all_records.extend(records)

    # ── Output ────────────────────────────────────────────────────────────────

    master = {
        "_meta": {
            "description":   "Zwei Länder Skiarena — merged daily weather data, Dec 10 → Apr 10",
            "resorts":       RESORTS,
            "resort_meta":   RESORT_META,
            "total_records": len(all_records),
            "date_range":    f"{EXPECTED[0]} → {EXPECTED[-1]}",
            "days_per_resort": len(EXPECTED),
            "source":        "SYNTHETIC (replace with Open-Meteo ERA5-Land)",
            "units": {
                "temperature":   "°C",
                "depth":         "cm",
                "snow":          "cm",
                "rain/precip":   "mm",
                "sunshine":      "hours",
                "wind":          "km/h",
                "freezing_level":"m above sea level",
            }
        },
        "records": all_records,
    }

    out_path = OUT_DIR / "master_data.json"
    with open(out_path, "w") as f:
        json.dump(master, f, separators=(",", ":"))  # compact

    qr_path = OUT_DIR / "data_quality_report.txt"
    with open(qr_path, "w") as f:
        f.write("Zwei Länder Skiarena — Data Quality Report\n")
        f.write(f"Total records: {len(all_records)}\n")
        f.write("\n".join(quality_lines))

    size_kb = out_path.stat().st_size / 1024
    print(f"\n  master_data.json  →  {size_kb:.0f} KB  ({len(all_records)} total records)")
    print(f"  data_quality_report.txt  →  written")
    print("\n  Step 2 complete. Seasons 2019/20–2024/25. Ready for Step 3 → compute_metrics.py")

if __name__ == "__main__":
    main()
