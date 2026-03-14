#!/usr/bin/env python3
"""
Step 2 — Clean, Normalize & QC Pipeline
Enforces the Strict Winter Mandate (Nov 1 - May 1). Drops summer slop.
Merges base and summit data while rigorously checking for missing API variables.
Zero inference: extracts sunshine duration explicitly from API facts.
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

        def safe_val(key):
            v = daily.get(key, [])
            return v[i] if i < len(v) and v[i] is not None else None

        flags = []
        t_max = safe_val("temperature_2m_max")
        t_min = safe_val("temperature_2m_min")
        snow_sum = safe_val("snowfall_sum")
        precip = safe_val("precipitation_sum")
        gusts = safe_val("wind_gusts_10m_max")
        wc = safe_val("weathercode")

        if t_max is None or t_min is None: flags.append("MISSING_TEMP")
        if snow_sum is None: flags.append("MISSING_SNOW_SUM")
        if precip is None: flags.append("MISSING_PRECIP")

        # -- HARD TRUTH MANDATE: SUNSHINE --
        # Open-Meteo ERA5-Land provides 'sunshine_duration' in seconds.
        # We perform zero inference. If the sensor payload is null, we log 0.
        raw_sun_seconds = safe_val("sunshine_duration") 
        
        if raw_sun_seconds is not None:
            sun_hours = round(raw_sun_seconds / 3600.0, 1)
        else:
            sun_hours = 0.0
            flags.append("MISSING_SUN_SENSOR_DATA")

        record = {
            "date": d,
            "tMax": t_max,
            "tMin": t_min,
            "snowfall_sum": snow_sum,
            "snow_depth": safe_val("snow_depth"),
            "precipitation_sum": precip,
            "precipitation_hours": safe_val("precipitation_hours"),
            "sunshine_duration": sun_hours, # Pure API reflection
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

    out_file = OUT_DIR / "enriched_data.json"
    with open(out_file, "w") as f:
        json.dump({"meta": {"generated_at": datetime.now().isoformat()}, "records": all_records}, f, separators=(",", ":"))
    print(f"Cleaned & normalized {len(all_records)} daily records.")

if __name__ == "__main__":
    main()
