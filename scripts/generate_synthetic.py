#!/usr/bin/env python3
"""
Synthetic data generator — produces realistic ERA5-style daily weather data
for the Zwei Länder Skiarena, Nov–Apr seasons 2019/20 through 2025/26.

Grounded in published climatological norms for each resort.
Replace with real Open-Meteo output once you run fetch_openmeteo.py.

Output: ../data/raw/{resort}_{base|summit}_raw.json (10 files, same schema as real API)
"""

import json
import random
import math
from pathlib import Path
from datetime import date, timedelta

random.seed(42)  # reproducible
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Date range ────────────────────────────────────────────────────────────────

def ski_dates():
    """All Nov 1 – Apr 30 dates for seasons 2019/20 through 2025/26 inclusive."""
    dates = []
    for start_year in range(2019, 2026):  # season start years
        d = date(start_year, 11, 1)
        end = date(start_year + 1, 4, 30)
        while d <= end:
            dates.append(d)
            d += timedelta(days=1)
    return dates

ALL_DATES = ski_dates()

# ── Resort climate profiles ───────────────────────────────────────────────────
# Values grounded in published data: Bergfex, Skiresort.info, snow-forecast.com,
# ZAMG climate normals for Tyrol / South Tyrol.
#
# Format per month (Jan=1..Apr=4):
#   base_temp_mean_c, base_temp_range_c,
#   summit_temp_mean_c, summit_temp_range_c,
#   base_snow_depth_mean_cm, summit_snow_depth_mean_cm,
#   sunshine_hours_mean, wind_max_mean_kmh,
#   snowfall_event_prob,   # prob of snowfall on any given day
#   snowfall_amount_mean,  # cm when it snows
#   rain_prob_base         # prob of rain at base

PROFILES = {
    "nauders": {
        # North-facing, high altitude, cold & powder-preserving
        # Base: 1400m  Summit: 2750m
        11: (-3.0, 8,  -11.0, 7,  30,  80, 3.5, 26, 0.30, 12, 0.01),
        12: (-5.0, 8,  -13.5, 7,  55, 130, 3.5, 27, 0.29, 13, 0.01),
        1:  (-5.5, 8,  -14.0, 7,  80, 160, 4.5, 28, 0.28, 14, 0.02),
        2:  (-4.5, 9,  -13.0, 7, 100, 185, 5.5, 25, 0.24, 12, 0.03),
        3:  (-1.5, 10, -10.0, 8,  90, 170, 6.5, 27, 0.22, 10, 0.06),
        4:  ( 3.5, 11,  -5.0, 9,  45, 110, 7.0, 24, 0.18,  8, 0.14),
    },
    "schoeneben": {
        # South-West facing, sun-drenched, warmer base, Lake Resia views
        # Base: 1460m  Summit: 2390m
        11: (-1.5, 9,   -9.0, 7,  20,  55, 4.5, 21, 0.26, 10, 0.02),
        12: (-3.5, 9,  -11.0, 7,  45, 105, 4.0, 22, 0.25, 11, 0.02),
        1:  (-4.0, 9,  -11.5, 7,  70, 130, 5.5, 22, 0.24, 12, 0.04),
        2:  (-2.5, 10, -10.0, 7,  85, 155, 6.5, 20, 0.20, 10, 0.06),
        3:  ( 1.0, 11,  -6.5, 8,  70, 135, 7.5, 22, 0.18,  9, 0.10),
        4:  ( 5.5, 12,  -1.5, 9,  30,  80, 8.0, 20, 0.14,  7, 0.20),
    },
    "watles": {
        # South-facing, consistently sunniest resort in South Tyrol
        # Base: 1500m  Summit: 2550m
        11: (-1.0, 9,   -8.5, 7,  18,  45, 5.0, 19, 0.23,  9, 0.02),
        12: (-3.0, 9,  -10.5, 7,  40,  95, 4.5, 20, 0.23, 10, 0.02),
        1:  (-3.5, 9,  -11.0, 7,  65, 120, 6.0, 20, 0.22, 11, 0.04),
        2:  (-1.5, 10,  -9.0, 7,  75, 140, 7.0, 18, 0.18,  9, 0.07),
        3:  ( 2.0, 11,  -5.5, 8,  60, 115, 8.0, 19, 0.16,  8, 0.12),
        4:  ( 6.5, 12,  -0.5, 9,  22,  65, 8.5, 17, 0.12,  6, 0.24),
    },
    "sulden": {
        # North-facing, Ortler glacier shadow, highest resort, coldest & deepest snow
        # Base: 1900m  Summit: 3250m
        11: (-5.0, 8,  -14.5, 8,  35,  90, 3.0, 30, 0.32, 14, 0.01),
        12: (-7.0, 8,  -16.5, 8,  80, 175, 3.5, 31, 0.31, 16, 0.01),
        1:  (-7.5, 8,  -17.0, 8, 110, 220, 4.0, 32, 0.30, 18, 0.01),
        2:  (-6.5, 9,  -16.0, 8, 130, 255, 5.0, 29, 0.26, 15, 0.02),
        3:  (-3.5, 10, -13.0, 9, 120, 235, 6.0, 31, 0.24, 13, 0.04),
        4:  ( 1.0, 11,  -7.5, 9,  75, 165, 6.5, 28, 0.20, 10, 0.10),
    },
    "trafoi": {
        # NE facing, Stelvio Pass shadow zone, windiest of the five
        # Base: 1540m  Summit: 2800m
        11: (-3.0, 9,  -12.0, 8,  25,  70, 3.0, 40, 0.30, 12, 0.01),
        12: (-5.0, 9,  -14.0, 8,  65, 140, 3.5, 41, 0.29, 14, 0.01),
        1:  (-5.5, 9,  -14.5, 8,  85, 165, 4.0, 42, 0.28, 15, 0.02),
        2:  (-4.5, 9,  -13.5, 8, 100, 188, 4.8, 39, 0.24, 13, 0.03),
        3:  (-1.5, 10, -10.5, 9,  90, 170, 5.5, 44, 0.22, 11, 0.06),
        4:  ( 3.0, 11,  -5.5, 9,  48, 115, 6.0, 40, 0.18,  8, 0.14),
    },
}

# Inter-annual variability factors (2019–2026)
YEAR_FACTORS = {
    # (temp_offset_c, snow_depth_factor, sunshine_factor)
    2019: (+0.3,  0.95, 1.02),   # near-average season start
    2020: (+0.5,  0.90, 1.05),   # slightly warm, below avg snow
    2021: (-1.0,  1.25, 0.90),   # cold excellent powder year
    2022: (+1.5,  0.75, 1.15),   # notably warm, poor snow, sunny
    2023: (-0.5,  1.10, 1.05),   # good season
    2024: (+0.8,  0.85, 1.10),   # warm-ish, decent
    2025: (-0.5,  1.15, 0.95),   # good season, average sun
    2026: (-0.2,  1.05, 1.00),   # tracking slightly above avg (partial)
}

# Month-specific practical sunshine caps at 47°N (Alps)
# = 82% of astronomical daylight (accounts for terrain shadows and scattering)
SUN_CAP_HOURS = {11: 7.5, 12: 7.0, 1: 7.5, 2: 8.5, 3: 9.5, 4: 11.0}

# ── WMO weathercode mapping (simplified) ─────────────────────────────────────

def weathercode(snow_cm, rain_mm, sunshine_h, wind_kmh):
    if rain_mm > 2:
        return 63 if rain_mm < 10 else 65    # moderate/heavy rain
    if snow_cm > 15:
        return 75    # heavy snowfall
    if snow_cm > 5:
        return 73    # moderate snowfall
    if snow_cm > 0:
        return 71    # slight snowfall
    if sunshine_h > 6:
        return 0     # clear sky
    if sunshine_h > 3:
        return 2     # partly cloudy
    return 3         # overcast (including gusty-clear days — WMO 55 is drizzle, wrong here)

# ── Smooth snow depth with seasonal curve ─────────────────────────────────────

def snow_depth_seasonal(day_of_year, base_cm):
    """
    Snow depth peaks mid-Feb (day ~46) and declines through April.
    Returns a multiplier on the monthly mean.
    """
    peak_day = 46  # mid Feb
    # Simple cosine curve: max at peak, 0.4× at day 1, 0.2× at day 120
    phase = (day_of_year - peak_day) / 90 * math.pi
    return max(0.15, 1.0 - 0.6 * (1 - math.cos(phase)) / 2)

# ── Core generator ─────────────────────────────────────────────────────────────

def generate_resort_elevation(resort_name, elevation_label):
    profile = PROFILES[resort_name]
    is_summit = (elevation_label == "summit")

    daily = {
        "time": [],
        "temperature_2m_max": [],
        "temperature_2m_min": [],
        "apparent_temperature_min": [],  # was missing
        "snowfall_sum": [],
        "snow_depth": [],                # stored in metres (API convention)
        "precipitation_sum": [],
        "precipitation_hours": [],       # was missing
        "rain_sum": [],
        "sunshine_duration": [],         # stored in seconds (API convention)
        "shortwave_radiation_sum": [],   # was missing (MJ/m²)
        "wind_speed_10m_max": [],        # replaces deprecated windspeed_10m_max
        "wind_gusts_10m_max": [],        # was missing
        "weather_code": [],              # replaces deprecated weathercode
        "uv_index_max": [],
    }

    prev_depth_m = None
    prev_date = None

    for d in ALL_DATES:
        m = d.month
        doy = d.timetuple().tm_yday
        y = d.year

        # Bug 2 fix: reset snow depth at season boundary (May→Nov gap = summer melt)
        if prev_date is not None and (d - prev_date).days > 7:
            prev_depth_m = None
        prev_date = d

        (bt_mean, bt_range, st_mean, st_range,
         b_depth, s_depth,
         sun_mean, wind_mean,
         snow_prob, snow_amount, rain_prob) = profile[m]

        t_off, snow_f, sun_f = YEAR_FACTORS[y]

        # Choose base or summit parameters
        if is_summit:
            t_mean   = st_mean + t_off
            t_range  = st_range
            depth_cm = s_depth * snow_f
        else:
            t_mean   = bt_mean + t_off
            t_range  = bt_range
            depth_cm = b_depth * snow_f

        # Temperature with daily noise
        noise_t = random.gauss(0, 1.8)
        t_max = t_mean + t_range / 2 + noise_t
        t_min = t_mean - t_range / 2 + noise_t

        # Snow event
        snow_today_cm = 0.0
        if random.random() < snow_prob:
            snow_today_cm = max(0, random.gauss(snow_amount, snow_amount * 0.5))

        # Rain (only if base temp > -1)
        rain_today_mm = 0.0
        if not is_summit and t_max > -1 and random.random() < rain_prob:
            rain_today_mm = max(0, random.gauss(4, 2))

        # Bug 1 fix: precipitation_sum is mm SWE; snow height cm → SWE mm = snow / 7
        precip_mm = rain_today_mm + snow_today_cm / 7

        # Snow depth — smooth seasonal curve + fresh snow contribution
        seasonal_mult = snow_depth_seasonal(doy, depth_cm)
        target_depth_cm = depth_cm * seasonal_mult + snow_today_cm * 0.5
        if prev_depth_m is None:
            cur_depth_cm = target_depth_cm + random.gauss(0, 5)
        else:
            # Gradual convergence + fresh snow; rain melts ~3 cm
            melt = rain_today_mm * 0.3
            cur_depth_cm = prev_depth_m * 100 * 0.97 + snow_today_cm * 0.6 - melt
            cur_depth_cm = max(0, cur_depth_cm)
            # Nudge toward seasonal trend
            cur_depth_cm = cur_depth_cm * 0.95 + target_depth_cm * 0.05
        prev_depth_m = max(0, cur_depth_cm) / 100

        # Bug 4 fix: use month-specific astronomical sun cap instead of hardcoded 10h
        sun_cap = SUN_CAP_HOURS[m]
        sun_noise = random.gauss(0, 1.5)
        sun_hours = max(0, min(sun_cap, sun_mean * sun_f + sun_noise))
        # Snow days are cloudier
        if snow_today_cm > 10:
            sun_hours = max(0, sun_hours - 2)
        sun_seconds = sun_hours * 3600

        # Wind — higher on storm days, elevated at Trafoi
        wind_noise = random.gauss(0, 10)
        wind_base = wind_mean + wind_noise
        if snow_today_cm > 15:
            wind_base += 15
        wind_kmh = max(5, wind_base)

        # UV — higher on sunny days, higher elevation
        uv_base = 2.5 + (m - 1) * 0.8 + sun_hours * 0.3
        if is_summit:
            uv_base *= 1.25
        uv = round(max(0.5, uv_base + random.gauss(0, 0.3)), 1)

        # Apparent minimum temperature (feels-colder from wind chill)
        apparent_t_min = round(t_min - wind_kmh * 0.05, 1)  # simplified wind chill offset

        # Precipitation hours — proportional to event intensity
        precip_h = 0.0
        if snow_today_cm > 0 or rain_today_mm > 0:
            intensity = snow_today_cm + rain_today_mm
            precip_h = round(min(18.0, 1.0 + intensity * 0.4 + random.gauss(0, 0.5)), 1)
            precip_h = max(0.0, precip_h)

        # Shortwave radiation — proxy from sunshine hours (MJ/m²/day; ~0.75 MJ per sun-hour at alpine elev)
        srad = round(max(0, sun_hours * 0.75 + random.gauss(0, 0.5)), 2)

        # WMO code
        wcode = weathercode(snow_today_cm, rain_today_mm, sun_hours, wind_kmh)

        # Store
        daily["time"].append(d.isoformat())
        daily["temperature_2m_max"].append(round(t_max, 1))
        daily["temperature_2m_min"].append(round(t_min, 1))
        daily["apparent_temperature_min"].append(apparent_t_min)
        daily["snowfall_sum"].append(round(snow_today_cm, 1))
        daily["snow_depth"].append(round(max(0, cur_depth_cm) / 100, 3))  # metres
        daily["precipitation_sum"].append(round(precip_mm, 1))
        daily["precipitation_hours"].append(precip_h)
        daily["rain_sum"].append(round(rain_today_mm, 1))
        daily["sunshine_duration"].append(round(sun_seconds, 0))
        daily["shortwave_radiation_sum"].append(srad)
        daily["wind_speed_10m_max"].append(round(wind_kmh * 0.75, 1))  # mean ≈ 75% of gust
        daily["wind_gusts_10m_max"].append(round(wind_kmh, 1))
        daily["weather_code"].append(wcode)
        daily["uv_index_max"].append(uv)

    return {
        "latitude":   0,
        "longitude":  0,
        "elevation":  0,
        "daily_units": {
            "time": "iso8601",
            "temperature_2m_max": "°C",
            "temperature_2m_min": "°C",
            "apparent_temperature_min": "°C",
            "snowfall_sum": "cm",
            "snow_depth": "m",
            "precipitation_sum": "mm",
            "precipitation_hours": "h",
            "rain_sum": "mm",
            "sunshine_duration": "s",
            "shortwave_radiation_sum": "MJ/m²",
            "wind_speed_10m_max": "km/h",
            "wind_gusts_10m_max": "km/h",
            "weather_code": "wmo code",
            "uv_index_max": "",
        },
        "daily": daily,
        "_meta": {
            "source": "SYNTHETIC — replace with real Open-Meteo ERA5-Land data",
            "resort": resort_name,
            "elevation_label": elevation_label,
            "days": len(daily["time"]),
        }
    }

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Generating synthetic weather data")
    print(f"Resorts: {list(PROFILES.keys())}")
    print(f"Dates: {ALL_DATES[0]} → {ALL_DATES[-1]}  ({len(ALL_DATES)} days)")
    print("=" * 60)

    for resort in PROFILES:
        for label in ("base", "summit"):
            random.seed(hash(resort + label) % 99991)  # reproducible per resort
            data = generate_resort_elevation(resort, label)
            out = OUTPUT_DIR / f"{resort}_{label}_raw.json"
            with open(out, "w") as f:
                json.dump(data, f)
            n = data["_meta"]["days"]
            print(f"  {resort:12} {label:7}  {n} days  → {out.name}")

    print("\nDone. 10 files written to data/raw/")
    print("NOTE: Data is SYNTHETIC for development. Run fetch_openmeteo.py for real data.")

if __name__ == "__main__":
    main()
