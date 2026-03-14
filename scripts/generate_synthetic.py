#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Synthetic Data Generator
Builds baseline Open-Meteo formatted JSONs for local testing.
Fixed: Nov/Dec date generation & accurate sun seconds logic.
"""

import json, random
from pathlib import Path
from datetime import date, timedelta

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def ski_dates():
    """Generates contiguous winter seasons from Nov 1 to Apr 30"""
    dates = []
    for year in range(2019, 2026):
        # Phase 1: Nov 1 to Dec 31
        d = date(year, 11, 1)
        while d <= date(year, 12, 31):
            dates.append(d)
            d += timedelta(days=1)
            
        # Phase 2: Jan 1 to Apr 30
        d = date(year + 1, 1, 1)
        while d <= date(year + 1, 4, 30):
            dates.append(d)
            d += timedelta(days=1)
    return dates

ALL_DATES = ski_dates()
RESORTS = ["nauders", "schoeneben", "watles", "sulden", "trafoi"]

def generate_resort(resort, label):
    daily = {
        "time": [], "temperature_2m_max": [], "temperature_2m_min": [],
        "apparent_temperature_min": [], "snowfall_sum": [], "snow_depth": [],
        "precipitation_sum": [], "precipitation_hours": [], "sunshine_duration": [],
        "shortwave_radiation_sum": [], "windspeed_10m_max": [],
        "wind_gusts_10m_max": [], "weathercode": []
    }
    
    snow_depth = random.uniform(0.1, 0.5)
    
    for d in ALL_DATES:
        daily["time"].append(d.isoformat())
        m = d.month
        
        # Base temps per month
        if m in [12, 1, 2]: base_t = -2
        elif m in [11, 3]: base_t = 2
        else: base_t = 5
        
        if label == "summit": base_t -= 6
        
        # Daily noise
        t_max = base_t + random.uniform(-3, 5)
        t_min = t_max - random.uniform(4, 8)
        
        is_storm = random.random() < 0.25
        
        if is_storm:
            precip = random.uniform(5, 25)
            pHours = random.uniform(4, 12)
            sun_seconds = 0.0 # Perfectly overcast
            sw = random.uniform(1.0, 4.0)
            wc = random.choice([71, 73, 75, 85]) # Snowing
            if t_max > 2: wc = random.choice([61, 63, 65]) # Raining
            gust = random.uniform(40, 90)
        else:
            precip = 0.0
            pHours = 0.0
            sun_hours = random.uniform(6.0, 11.0)
            sun_seconds = sun_hours * 3600.0 # MUST BE SECONDS FOR JS ENGINE
            sw = sun_hours * 1.5
            wc = random.choice([0, 1, 2])
            gust = random.uniform(10, 30)
            
        snowfall = 0.0
        if precip > 0 and t_max <= 2.0:
            snowfall = precip * random.uniform(0.8, 1.2)
            
        snow_depth += (snowfall / 100.0) # cm to m
        snow_depth -= 0.01 # settle
        if snow_depth < 0: snow_depth = 0.0
        
        daily["temperature_2m_max"].append(round(t_max, 1))
        daily["temperature_2m_min"].append(round(t_min, 1))
        daily["apparent_temperature_min"].append(round(t_min - 3, 1))
        daily["snowfall_sum"].append(round(snowfall, 1))
        daily["snow_depth"].append(round(snow_depth, 2))
        daily["precipitation_sum"].append(round(precip, 1))
        daily["precipitation_hours"].append(round(pHours, 1))
        daily["sunshine_duration"].append(round(sun_seconds, 1))
        daily["shortwave_radiation_sum"].append(round(sw, 2))
        daily["windspeed_10m_max"].append(round(gust * 0.6, 1))
        daily["wind_gusts_10m_max"].append(round(gust, 1))
        daily["weathercode"].append(wc)

    return {"daily": daily, "_meta": {"days": len(ALL_DATES)}}

def main():
    for r in RESORTS:
        for el in ["base", "summit"]:
            data = generate_resort(r, el)
            out = OUTPUT_DIR / f"{r}_{el}_raw.json"
            with open(out, "w") as f:
                json.dump(data, f)
    print(f"✓ Synthetic data generated spanning Nov-Apr ({len(ALL_DATES)} days).")

if __name__ == "__main__":
    main()
