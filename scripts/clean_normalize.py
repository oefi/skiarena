#!/usr/bin/env python3
"""
Step 2 — Clean, Normalize & QC Pipeline
Enforces the Strict Winter Mandate (Nov 1 - May 1).
Infers missing sunshine data safely without destroying real storm days.
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
        d_obj = datetime.strptime(d, "%Y-%m-%d")
        m = d_obj.month
        if m not in [11, 12, 1, 2, 3, 4] and not (m == 5 and d_obj.day == 1):
            continue

        def safe_val(key):
            arr = daily.get(key)
            return arr[i] if arr and i < len(arr) else None

        t_max  = safe_val("temperature_2m_max")
        t_min  = safe_val("temperature_2m_min")
        gusts  = safe_val("wind_gusts_10m_max")
        precip = safe_val("precipitation_sum")
        snow   = safe_val("snowfall_sum")
        sun    = safe_val("sunshine_duration")
        sw     = safe_val("shortwave_radiation_sum")
        wc     = safe_val("weathercode")

        flags = []
        
        if precip == 0.0 and snow is None:
            snow = 0.0
            flags.append("snow_inferred")

        # Crucial Fix: Only infer if sun is literally missing (None). Allow 0.0 to stay 0.0!
        if sun is None:
            if sw is not None and sw > 2.0:
                est_hours = (sw - 2.5) * 0.8
                sun = max(0.0, min(est_hours, 12.0)) * 3600.0
                flags.append("sun_inferred_from_radiation")
            elif wc is not None:
                if wc <= 1: sun = 36000.0
                elif wc == 2: sun = 21600.0
                elif wc == 3: sun = 7200.0
                else: sun = 0.0
                flags.append("sun_inferred_from_wmo")
            else:
                sun = 0.0

        if t_max is None: flags.append("no_temp")
        if gusts is None: flags.append("no_wind")

        record = {
            "date": d,
            "temperature_2m_max": t_max,
            "temperature_2m_min": t_min,
            "apparent_temperature_min": safe_val("apparent_temperature_min"),
            "snowfall_sum": snow,
            "snow_depth": safe_val("snow_depth"),
            "precipitation_sum": precip,
            "precipitation_hours": safe_val("precipitation_hours"),
            "sunshine_duration": sun,
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
    master = {"_meta": {"resorts": RESORTS, "total_records": len(all_records)}, "records": all_records}
    with open(OUT_DIR / "master_data.json", "w") as f:
        json.dump(master, f, separators=(",", ":"))

if __name__ == "__main__":
    main()
