#!/usr/bin/env python3
"""
Step 3 — Compute Ski Quality Metrics (Piste-Optimised)
Reads master_data.json, computes per-record composite ski scores,
writes enriched_data.json consumed by build_dashboard.py.

Composite Bluebird Score (0.0–1.0) — optimised for piste skiers:

Normal conditions:
  45%  Sunshine duration (normalised vs resort historical peak)
  30%  Snow depth        (sigmoid curve: 50cm → 0.80, not 0.40)
  25%  Temperature       (date-aware seasonal bands — see below)
  Wind: global multiplier (0.3–1.0)
  +≤15% Powder bonus (additive, moderate fresh snow days)

Powder override  (fresh ≥ 15cm AND gust < 60 km/h):
  Sunshine weight → 0.0  (irrelevant during a storm)
  Depth weight    → 0.60  (powder coverage matters)
  Powder intensity→ 0.15  (scales with snowfall amount)
  Temperature     → 0.25  (unchanged)

Temperature bands (date-aware):
  Nov 1  – Feb 28:  optimal -12°C to -2°C  (deep winter)
  Mar 1  – Mar 20:  optimal  -6°C to +2°C  (transition)
  Mar 21 – May 1:   optimal  -2°C to +8°C  (Firn / spring skiing)
"""

import json
import math
from pathlib import Path

IN_FILE  = Path(__file__).parent.parent / "data" / "processed" / "master_data.json"
OUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "enriched_data.json"


# ── Per-resort normalization bounds (computed from full dataset) ──────────────

def compute_resort_bounds(records):
    """Compute min/max for normalizable variables per resort."""
    bounds = {}
    for r in records:
        resort = r["resort"]
        if resort not in bounds:
            bounds[resort] = {
                "sun_vals": [], "depth_vals": [],
                "temp_vals": [], "gust_vals": [],
            }
        b = r.get("base", {}) or {}
        s = r.get("summit", {}) or {}

        sun = b.get("sunshine_duration")
        if sun is not None:
            bounds[resort]["sun_vals"].append(sun)

        depth = s.get("snow_depth")
        if depth is not None:
            bounds[resort]["depth_vals"].append(depth)

        temp = b.get("temperature_2m_max")
        if temp is not None:
            bounds[resort]["temp_vals"].append(temp)

        gust = b.get("wind_gusts_10m_max")
        if gust is not None:
            bounds[resort]["gust_vals"].append(gust)

    normalized = {}
    for resort, bd in bounds.items():
        normalized[resort] = {
            "sun":   safe_range(bd["sun_vals"]),
            "depth": safe_range(bd["depth_vals"]),
            "temp":  safe_range(bd["temp_vals"]),
            "gust":  safe_range(bd["gust_vals"]),
        }
    return normalized


def norm(val, lo, hi):
    """Clamp-normalize value into [0, 1]."""
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))


def safe_range(vals):
    """Return (min, max) tuple; falls back to (0, 1) if vals is empty."""
    if not vals:
        return (0.0, 1.0)
    return (min(vals), max(vals))


def depth_score_piste(depth_m, resort):
    """
    Sigmoid-shaped depth scoring for piste skiers (80% of resort visitors).
    A groomed 50cm base skis identically to 200cm on a prepared trail.
    Linear normalisation up to 150cm penalises perfectly adequate conditions.

    Curve:
      0–20cm  : 0.00–0.20  (rocks, grass — genuinely dangerous)
      20–50cm : 0.20–0.80  (rapid climb — marginal to adequate piste)
      50cm+   : 0.80–1.00  (diminishing returns — groomed corduroy is groomed corduroy)
    """
    if depth_m is None:
        return 0.0
    cm = depth_m * 100
    if cm <= 0:
        return 0.0
    if cm <= 20:
        return 0.20 * (cm / 20)
    if cm <= 50:
        return 0.20 + 0.60 * ((cm - 20) / 30)
    # 50cm+ : logarithmic approach to 1.0; ~0.95 at 150cm, ~0.98 at 250cm
    return min(1.0, 0.80 + 0.20 * math.log(1 + (cm - 50) / 50) / math.log(3))


def temperature_score_seasonal(t_max, date_str):
    """
    Temperature scoring with date-aware seasonal bands.

    A skier's thermal comfort shifts as the season progresses.
    What is "dangerously warm" in January is "perfect spring skiing" in April.

      Phase 1 — Deep winter   (Nov 1 – Feb 28):  optimal -12°C to -2°C
      Phase 2 — Transition    (Mar 1 – Mar 20):  optimal  -6°C to +2°C
      Phase 3 — Spring skiing (Mar 21 – May 1):  optimal  -2°C to +8°C
                                                 (Firn skiing — corn snow,
                                                  sun terraces, t-shirts)
    """
    if t_max is None:
        return 0.5
    try:
        month = int(date_str[5:7])
        day   = int(date_str[8:10])
    except (ValueError, IndexError):
        month, day = 1, 1

    if month >= 11 or month <= 2:
        opt_lo, opt_hi, too_warm = -12, -2, 15   # deep winter
    elif month == 3 and day <= 20:
        opt_lo, opt_hi, too_warm = -6,   2, 18   # transition
    else:
        opt_lo, opt_hi, too_warm = -2,   8, 22   # Firn / spring

    if t_max <= -20:
        return 0.3
    if t_max <= opt_lo:
        return 0.7 + 0.3 * (t_max - (-20)) / (opt_lo - (-20))
    if t_max <= opt_hi:
        return 1.0
    if t_max <= too_warm:
        return 1.0 - 0.6 * (t_max - opt_hi) / (too_warm - opt_hi)
    return 0.05


def wind_penalty(gust_kmh):
    """Global multiplier 0.3–1.0. Gusts above 50 km/h start hurting."""
    if gust_kmh is None:
        return 1.0
    if gust_kmh <= 30:
        return 1.0
    if gust_kmh <= 50:
        return 1.0 - 0.1 * (gust_kmh - 30) / 20
    if gust_kmh <= 80:
        return 0.9 - 0.4 * (gust_kmh - 50) / 30
    return 0.3


def powder_bonus(snowfall_cm, gust_kmh):
    """
    Additive bonus up to +0.15. Requires fresh > 10cm AND gust < 50 km/h.
    Only fires when powder_override is False (i.e. moderate powder days).
    """
    if snowfall_cm is None or snowfall_cm < 10:
        return 0.0
    snow_factor = min(1.0, (snowfall_cm - 10) / 20)
    if gust_kmh is None or gust_kmh <= 30:
        wind_factor = 1.0
    elif gust_kmh <= 50:
        wind_factor = 1.0 - (gust_kmh - 30) / 20
    else:
        wind_factor = 0.0
    return round(0.15 * snow_factor * wind_factor, 4)


def compute_score(record, bounds):
    """
    Piste-skier-optimised composite score (0.0–1.0).

    Normal conditions:
      45%  Sunshine duration (normalised vs resort peak)
      30%  Snow depth        (sigmoid — 50cm → 80%, not 40%)
      25%  Temperature       (date-aware seasonal bands)
      Wind: global multiplier

    Powder override  (fresh >= 15cm AND gust < 60 km/h):
      Sunshine weight → 0% (irrelevant during a storm)
      Depth weight    → 60% (powder depth matters for coverage)
      Powder intensity→ 15% (scales with snowfall amount)
      Temperature     → 25% (unchanged)
      Result: a 30cm storm day scores 0.85+, not 0.65.
    """
    b         = record.get("base", {}) or {}
    s         = record.get("summit", {}) or {}
    resort    = record["resort"]
    date_str  = record.get("date", "2000-01-01")
    rb        = bounds.get(resort, {})

    sun   = b.get("sunshine_duration")
    depth = s.get("snow_depth")
    t_max = b.get("temperature_2m_max")
    gust  = b.get("wind_gusts_10m_max")
    fresh = s.get("snowfall_sum")

    if t_max is None and depth is None:
        return {"score": None, "metrics": {
            "fSun": 0, "fDepth": 0, "fTemp": 0,
            "windMult": 1, "powderBonus": 0, "powderOverride": False
        }}

    sun_range = rb.get("sun", (0, 1))
    # Redistribute weights when sunshine_duration is null (rare API anomaly at
    # sequence boundaries). Using 0 would silently crush 45% of the score on
    # days that may be perfectly skiable. Instead, spread its weight to the
    # remaining factors that do have data.
    if sun is not None:
        f_sun = max(0.0, min(1.0, norm(sun, *sun_range)))
        w_sun, w_depth, w_temp = 0.45, 0.30, 0.25
    else:
        f_sun  = 0.0
        w_sun, w_depth, w_temp = 0.00, 0.55, 0.45  # 45% redistributed to depth+temp

    f_depth   = depth_score_piste(depth, resort)
    f_temp    = temperature_score_seasonal(t_max, date_str)
    w_mult    = wind_penalty(gust)
    p_bonus   = powder_bonus(fresh, gust)

    fresh_cm       = fresh if fresh is not None else 0
    gust_kmh       = gust  if gust  is not None else 0
    powder_override = fresh_cm >= 15 and gust_kmh < 60

    if powder_override:
        powder_intensity = min(1.0, fresh_cm / 40)   # 1.0 at 55cm+
        raw = 0.0 * f_sun + 0.60 * f_depth + 0.25 * f_temp + 0.15 * powder_intensity
        p_bonus = 0.0  # override already captures it
    else:
        raw = w_sun * f_sun + w_depth * f_depth + w_temp * f_temp

    score = round(max(0.0, min(1.0, (raw + p_bonus) * w_mult)), 4)

    return {
        "score": score,
        "metrics": {
            "fSun":          round(f_sun, 4),
            "fDepth":        round(f_depth, 4),
            "fTemp":         round(f_temp, 4),
            "windMult":      round(w_mult, 4),
            "powderBonus":   round(p_bonus, 4),
            "powderOverride": powder_override,
        }
    }

def main():
    print("Computing ski quality metrics…")

    with open(IN_FILE, "r") as f:
        master = json.load(f)

    records = master["records"]
    bounds  = compute_resort_bounds(records)

    enriched = []
    scored = 0
    for r in records:
        score_data = compute_score(r, bounds)
        enriched_rec = dict(r)
        enriched_rec["score"]   = score_data["score"]
        enriched_rec["metrics"] = score_data["metrics"]
        enriched.append(enriched_rec)
        if score_data["score"] is not None:
            scored += 1

    out = {
        "_meta": {
            **master["_meta"],
            "scored_records": scored,
            "score_weights": {"normal": {"sun": 0.45, "depth": 0.30, "temp": 0.25}, "powder_override": {"trigger": "fresh >= 15cm AND gust < 60 km/h", "depth": 0.60, "sun": 0.0}, "depth_curve": "sigmoid piste-optimised", "temp_bands": "date-aware seasonal"},
        },
        "records": enriched,
    }

    with open(OUT_FILE, "w") as f:
        json.dump(out, f, separators=(",", ":"))

    print(f"  → {OUT_FILE.name}  ({scored}/{len(enriched)} records scored)")
    print("Done.")


if __name__ == "__main__":
    main()
