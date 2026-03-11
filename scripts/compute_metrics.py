#!/usr/bin/env python3
"""
Step 3 — Compute Derived Metrics
Applies the rigorous empirical Ski Quality Score algorithms to the cleaned data.
"""

import json
from pathlib import Path

IN_PATH  = Path(__file__).parent.parent / "data" / "processed" / "master_data.json"
OUT_PATH = Path(__file__).parent.parent / "data" / "processed" / "enriched_data.json"

def compute_universal_score(summit, base):
    if not summit or not base: return 0.0

    depth_cm = (summit.get("snow_depth") or 0) * 100
    fresh_cm = summit.get("snowfall_sum") or 0
    t_max = summit.get("temperature_2m_max")
    
    wind_gust = summit.get("wind_gusts_10m_max") or 0
    wind_chill = summit.get("apparent_temperature_min") or t_max or 0
    sun_seconds = summit.get("sunshine_duration") or 0
    
    base_snow = min(depth_cm / 80.0, 1.0) 
    base_temp = 1.0 if t_max is not None and -12 <= t_max <= -2 else 0.5
    
    score = (base_snow * 0.5) + (base_temp * 0.5)
    
    if fresh_cm >= 15: score += 0.3
    elif fresh_cm >= 5: score += 0.15
    if sun_seconds > 18000: score += 0.1
    
    if wind_gust > 75: score *= 0.2
    elif wind_gust > 55: score *= 0.6
    if wind_chill < -22: score *= 0.7
        
    rain = base.get("precipitation_sum", 0) - (base.get("snowfall_sum", 0) / 10)
    if rain > 2 and t_max and t_max > 2: score *= 0.4

    return max(0.0, min(1.0, score))

def main():
    with open(IN_PATH, "r") as f: master = json.load(f)
    
    enriched = []
    for r in master["records"]:
        score = compute_universal_score(r.get("summit"), r.get("base"))
        r["score_universal"] = score
        enriched.append(r)
        
    master["records"] = enriched
    with open(OUT_PATH, "w") as f:
        json.dump(master, f, separators=(",", ":"))

if __name__ == "__main__":
    main()