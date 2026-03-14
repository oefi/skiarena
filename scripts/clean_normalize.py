scripts/clean_normalize.py
#!/usr/bin/env python3
"""
Step 2 — Clean, Normalize & QC Pipeline
Enforces the Strict Winter Mandate (Nov 1 - May 1). Drops summer slop.
Merges base and summit data while rigorously checking for missing API variables.
Infers missing sunshine data via WMO weather codes.
"""

import json
from pathlib import Path
from datetime import datetime

RAW_DIR  = Path(__file__).parent.parent / "data" / "raw"
OUT_DIR  = Path(__file__).parent.parent / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESORTS = ["nauders", "schoeneben", "watles", "sulden", "trafoi"]

def load_raw(resort, elevation):
    path = RAW_DIR / f"{resort}_{elevation}_raw.json"
    if not path.exists(): return None
    with open(path, "r") as f: return json.load(f)

def extract_daily(raw_json):
    if not raw_json or "daily" not in raw_json: return []
    daily = raw_json["daily"]
    extracted = []

    for i, d in enumerate(daily.get("time", [])):
        # STRICT WINTER MANDATE: Keep only Nov 1 through May 1. Purge the rest.
        d_obj = datetime.strptime(d, "%Y-%m-%d")
        m = d_obj.month
        if m not in [11, 12, 1, 2, 3, 4] and not (m == 5 and d_obj.day == 1):
            continue

        # Bomb-proof array indexing
        def safe_val(key):
            arr = daily.get(key)
            return arr[i] if arr and i < len(arr) else None

        t_max  = safe_val("temperature_2m_max")
        t_min  = safe_val("temperature_2m_min")
        gusts  = safe_val("wind_gusts_10m_max")
        precip = safe_val("precipitation_sum")
        snow   = safe_val("snowfall_sum")
        sun    = safe_val("sunshine_duration")
        wc     = safe_val("weathercode")

        flags = []
        
        # Physical Inference 1: 0 precip means mathematically 0 snow
        if precip == 0.0 and snow is None:
            snow = 0.0
            flags.append("snow_inferred")

        # Physical Inference 2: ERA5 drops sunshine duration randomly. Infer from WMO codes.
        if sun is None and wc is not None:
            if wc == 0:
                sun = 36000.0  # ~10 hours pure sun
            elif wc in [1, 2]:
                sun = 21600.0  # ~6 hours partial sun
            elif wc == 3:
                sun = 7200.0   # ~2 hours peek-a-boo
            else:
                sun = 0.0      # Overcast / Storm
            flags.append("sun_inferred")

        if t_max is None: flags.append("no_temp")
        if gusts is None: flags.append("no_wind")

        # -- UNITS NORMALIZATION --
        sun_hrs = round(sun / 3600.0, 1) if sun is not None else 0.0
        
        raw_snow = safe_val("snow_depth")
        snow_cm = round(raw_snow * 100.0, 1) if raw_snow is not None else 0.0

        record = {
            "date": d,
            "temperature_2m_max": t_max,
            "temperature_2m_min": t_min,
            "apparent_temperature_min": safe_val("apparent_temperature_min"),
            "snowfall_sum": snow,
            "snow_depth": snow_cm,
            "precipitation_sum": precip,
            "precipitation_hours": safe_val("precipitation_hours"),
            "sunshine_duration": sun_hrs,
            "windspeed_10m_max": safe_val("windspeed_10m_max"),
            "wind_gusts_10m_max": gusts,
            "weathercode": wc,
            "data_flags": flags
        }
        extracted.append(record)
    return extracted

def main():
    all_records = []
    for resort in RESORTS:
        base_raw = load_raw(resort, "base")
        summ_raw = load_raw(resort, "summit")
        if not base_raw or not summ_raw: continue

        base_data = {r["date"]: r for r in extract_daily(base_raw)}
        summ_data = {r["date"]: r for r in extract_daily(summ_raw)}

        intersect = sorted(list(set(base_data.keys()) & set(summ_data.keys())))
        for d in intersect:
            b_rec, s_rec = base_data[d], summ_data[d]
            all_records.append({
                "date": d, "resort": resort,
                "base": b_rec, "summit": s_rec,
                "flags": list(set(b_rec.get("data_flags", []) + s_rec.get("data_flags", [])))
            })

    all_records.sort(key=lambda x: (x["date"], x["resort"]))

    master = {
        "_meta": {"resorts": RESORTS, "total_records": len(all_records)},
        "records": all_records,
    }
    with open(OUT_DIR / "master_data.json", "w") as f:
        json.dump(master, f, separators=(",", ":"))

if __name__ == "__main__":
    main()
