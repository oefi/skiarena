#!/usr/bin/env python3
"""
Step 3 — Compute Ski Quality Metrics
Reads master_data.json, computes per-record composite ski scores,
writes enriched_data.json consumed by build_dashboard.py.

Composite Bluebird Score (0.0–1.0) weights:
  40%  Sunshine duration (normalized against resort 6-year peak)
  25%  Summit snow depth  (normalized against resort 6-year max)
  20%  Base temperature   (optimal range -12°C to -2°C)
  15%  Wind penalty       (gusts above 50 km/h start degrading score)
"""

import json
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


def temperature_score(t_max):
    """
    Optimal base temp for ski conditions: -12°C to -2°C → score 1.0
    Warm (>0°C) and extreme cold (<-20°C) degrade score.
    """
    if t_max is None:
        return 0.5  # neutral, no penalty for missing data
    if t_max <= -20:
        return 0.3  # too cold for lifts / exposed skin
    if t_max <= -12:
        return 0.7 + 0.3 * (t_max - (-20)) / 8   # rising through cold zone
    if t_max <= -2:
        return 1.0  # sweet spot
    if t_max <= 5:
        return 1.0 - 0.5 * (t_max - (-2)) / 7    # warm but survivable
    if t_max <= 15:
        return 0.5 - 0.4 * (t_max - 5) / 10      # slushy / icy melt cycles
    return 0.05  # genuinely disgusting


def wind_penalty(gust_kmh):
    """Returns a multiplier 0.3–1.0. Gusts above 50 km/h start hurting."""
    if gust_kmh is None:
        return 1.0
    if gust_kmh <= 30:
        return 1.0
    if gust_kmh <= 50:
        return 1.0 - 0.1 * (gust_kmh - 30) / 20   # mild degradation
    if gust_kmh <= 80:
        return 0.9 - 0.4 * (gust_kmh - 50) / 30   # lifts start closing
    return 0.3  # full storm, most lifts closed


def compute_score(record, bounds):
    """
    Returns dict: { score: float|None, metrics: { fSun, fDepth, fTemp, windMult } }
    score is None only if we have zero usable data.
    """
    b = record.get("base", {}) or {}
    s = record.get("summit", {}) or {}
    resort = record["resort"]
    rb = bounds.get(resort, {})

    sun   = b.get("sunshine_duration")
    depth = s.get("snow_depth")
    t_max = b.get("temperature_2m_max")
    gust  = b.get("wind_gusts_10m_max")

    # Require at least temp or depth to compute a score
    if t_max is None and depth is None:
        return {"score": None, "metrics": {"fSun": 0, "fDepth": 0, "fTemp": 0, "windMult": 1}}

    sun_range   = rb.get("sun",   (0, 1))
    depth_range = rb.get("depth", (0, 1))

    f_sun   = norm(sun   if sun   is not None else 0, *sun_range)
    f_depth = norm(depth if depth is not None else 0, *depth_range)
    f_temp  = temperature_score(t_max)
    w_mult  = wind_penalty(gust)

    # Weighted composite, wind acts as a global multiplier
    raw = (0.40 * f_sun) + (0.25 * f_depth) + (0.20 * f_temp) + (0.15 * w_mult)
    score = round(max(0.0, min(1.0, raw * w_mult)), 4)

    return {
        "score": score,
        "metrics": {
            "fSun":    round(f_sun, 4),
            "fDepth":  round(f_depth, 4),
            "fTemp":   round(f_temp, 4),
            "windMult": round(w_mult, 4),
        }
    }


# ── Main ──────────────────────────────────────────────────────────────────────

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
            "score_weights": {"sun": 0.40, "depth": 0.25, "temp": 0.20, "wind_mult": 0.15},
        },
        "records": enriched,
    }

    with open(OUT_FILE, "w") as f:
        json.dump(out, f, separators=(",", ":"))

    print(f"  → {OUT_FILE.name}  ({scored}/{len(enriched)} records scored)")
    print("Done.")


if __name__ == "__main__":
    main()
