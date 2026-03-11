#!/usr/bin/env python3
"""
Step 3 — Compute Derived Metrics
Reads master_data.json, enriches every record with Ski Quality Scores
(4 personas), flags, rankings, and pre-aggregates weekly/monthly
lookup tables consumed directly by the dashboard JS.

Output:
  ../data/processed/enriched_data.json
"""

import json
import math
from pathlib import Path
from collections import defaultdict
from datetime import date, timedelta

IN_PATH  = Path(__file__).parent.parent / "data" / "processed" / "master_data.json"
OUT_PATH = Path(__file__).parent.parent / "data" / "processed" / "enriched_data.json"

RESORTS = ["nauders", "schoeneben", "watles", "sulden", "trafoi"]

# ── Score component functions ─────────────────────────────────────────────────

def snow_score(depth_cm, fresh_cm):
    base = min(depth_cm / 80.0, 1.0)
    powder_bonus = 0.20 if fresh_cm >= 10 else (0.10 if fresh_cm >= 5 else 0.0)
    return min(1.0, base + powder_bonus)

def temp_score(summit_temp_max):
    """Ideal window: −5°C to −15°C summit. Degrades outside."""
    if summit_temp_max is None:
        return 0.5
    t = summit_temp_max
    if -15 <= t <= -5:
        return 1.0
    elif t > -5:
        # Too warm — slush risk. 0 at +5°C
        return max(0.0, 1.0 - (t + 5) / 10)
    else:
        # Too cold — frostbite territory. 0 at −30°C
        return max(0.0, 1.0 - (-15 - t) / 15)

def sun_score(sunshine_h):
    return min(sunshine_h / 8.0, 1.0)

def wind_penalty(wind_kmh):
    """0 below 40 km/h → −1.0 at 80+ km/h (lift closure territory)"""
    if wind_kmh <= 40:
        return 0.0
    return max(-1.0, -((wind_kmh - 40) / 40))

def rain_penalty(rain_mm, base_temp_max):
    """Rain at base = icy aftermath, unpleasant riding"""
    if rain_mm > 5 and base_temp_max > 0:
        return -0.25
    elif rain_mm > 1 and base_temp_max > 0:
        return -0.10
    return 0.0

# ── Persona weight sets ───────────────────────────────────────────────────────
# (snow_w, temp_w, sun_w, wind_w)

PERSONAS = {
    "universal": (0.35, 0.25, 0.25, 0.15),
    "powder":    (0.55, 0.25, 0.10, 0.10),
    "sun":       (0.20, 0.20, 0.50, 0.10),
    "family":    (0.25, 0.30, 0.35, 0.10),
}

def ski_score(record, persona="universal"):
    sw, tw, sunw, ww = PERSONAS[persona]
    ss = snow_score(record["summit_depth_cm"], record["fresh_snow_cm"])
    ts = temp_score(record["summit_temp_max"])
    sus = sun_score(record["sunshine_h"])
    wp  = wind_penalty(record["summit_wind_kmh"])
    rp  = rain_penalty(record["rain_mm"], record["base_temp_max"])
    raw = sw * ss + tw * ts + sunw * sus + ww * wp + rp
    return round(max(0.0, min(1.0, raw)), 4)

def score_to_stars(score):
    if score >= 0.85: return 5
    if score >= 0.70: return 4
    if score >= 0.55: return 3
    if score >= 0.40: return 2
    return 1

# ── Season key helpers ────────────────────────────────────────────────────────

def season_week_key(date_str):
    """Returns season-relative key e.g. '2024/25-W04'"""
    month  = int(date_str[5:7])
    year   = int(date_str[:4])
    s      = year if month == 12 else year - 1
    season = f"{s}/{str(s+1)[2:]}"
    d      = date.fromisoformat(date_str)
    dec10  = date(s, 12, 10)
    week   = (d - dec10).days // 7 + 1
    return f"{season}-W{week:02d}"

def get_season(date_str):
    month = int(date_str[5:7])
    year  = int(date_str[:4])
    s     = year if month == 12 else year - 1
    return f"{s}/{str(s+1)[2:]}"

def month_key(date_str):
    return date_str[:7]  # 'YYYY-MM'

# ── Enrich records ────────────────────────────────────────────────────────────

def enrich(records):
    enriched = []
    for r in records:
        e = dict(r)

        # Four persona scores
        for persona in PERSONAS:
            e[f"score_{persona}"] = ski_score(r, persona)

        # Stars (universal)
        e["stars"] = score_to_stars(e["score_universal"])

        # Powder day flag
        e["powder_day"] = r["fresh_snow_cm"] >= 10

        # Lift closure risk
        e["lift_risk"] = (
            r["summit_wind_kmh"] >= 60 or
            (r["rain_mm"] > 2 and r["base_temp_max"] > 0)
        )

        # Rain at base flag
        e["rain_at_base"] = r["rain_mm"] > 1 and r["base_temp_max"] > 0

        # Week / month / season keys
        e["week_key"]    = season_week_key(r["date"])
        e["month_key"]   = month_key(r["date"])
        e["season"]      = r.get("season") or get_season(r["date"])

        enriched.append(e)

    return enriched

# ── Pre-aggregations ──────────────────────────────────────────────────────────

def avg(vals):
    v = [x for x in vals if x is not None]
    return round(sum(v) / len(v), 4) if v else None

def aggregate_group(recs):
    """Summarise a list of daily records into one aggregate object."""
    return {
        "n":                   len(recs),
        "score_universal":     avg([r["score_universal"] for r in recs]),
        "score_powder":        avg([r["score_powder"]    for r in recs]),
        "score_sun":           avg([r["score_sun"]       for r in recs]),
        "score_family":        avg([r["score_family"]    for r in recs]),
        "stars":               round(avg([r["stars"]     for r in recs]), 1),
        "avg_base_depth_cm":   avg([r["base_depth_cm"]   for r in recs]),
        "avg_summit_depth_cm": avg([r["summit_depth_cm"] for r in recs]),
        "avg_fresh_snow_cm":   avg([r["fresh_snow_cm"]   for r in recs]),
        "avg_sunshine_h":      avg([r["sunshine_h"]      for r in recs]),
        "avg_wind_kmh":        avg([r["wind_kmh"]        for r in recs]),
        "avg_base_temp_max":   avg([r["base_temp_max"]   for r in recs]),
        "avg_summit_temp_max": avg([r["summit_temp_max"] for r in recs]),
        "powder_days":         sum(1 for r in recs if r["powder_day"]),
        "lift_risk_days":      sum(1 for r in recs if r["lift_risk"]),
        "rain_days":           sum(1 for r in recs if r["rain_at_base"]),
        "max_summit_depth_cm": max((r["summit_depth_cm"] for r in recs), default=None),
    }

def build_aggregations(enriched):
    """
    Returns dicts keyed by (resort, week_key), (resort, month_key),
    (resort, year), and cross-resort daily best.
    """
    # Group by resort
    by_resort = defaultdict(list)
    for r in enriched:
        by_resort[r["resort"]].append(r)

    weekly   = {}   # {resort: {week_key: aggregate}}
    monthly  = {}   # {resort: {month_key: aggregate}}
    seasonal = {}   # {resort: {season: aggregate}}

    for resort, recs in by_resort.items():
        # Weekly
        by_week = defaultdict(list)
        for r in recs:
            by_week[r["week_key"]].append(r)
        weekly[resort] = {wk: aggregate_group(v) for wk, v in by_week.items()}

        # Monthly
        by_month = defaultdict(list)
        for r in recs:
            by_month[r["month_key"]].append(r)
        monthly[resort] = {mk: aggregate_group(v) for mk, v in by_month.items()}

        # Seasonal
        by_season = defaultdict(list)
        for r in recs:
            by_season[r["season"]].append(r)
        seasonal[resort] = {s: aggregate_group(v) for s, v in by_season.items()}

    # ── Daily cross-resort ranking ──────────────────────────────────────────
    by_date = defaultdict(list)
    for r in enriched:
        by_date[r["date"]].append(r)

    daily_ranking = {}
    for dt, recs in by_date.items():
        ranking = {}
        for persona in PERSONAS:
            key      = f"score_{persona}"
            sorted_r = sorted(recs, key=lambda x: x[key], reverse=True)
            ranking[persona] = [r["resort"] for r in sorted_r]
        daily_ranking[dt] = ranking

    # ── Powder day calendar ─────────────────────────────────────────────────
    # {resort: {season: [list of powder day dates]}}
    powder_calendar = {}
    for resort, recs in by_resort.items():
        by_season = defaultdict(list)
        for r in recs:
            if r["powder_day"]:
                by_season[r["season"]].append(r["date"])
        powder_calendar[resort] = dict(by_season)

    # ── Season summary stats (per season per resort) ────────────────────────
    season_summary = {}
    for resort, recs in by_resort.items():
        seasons = sorted(set(r["season"] for r in recs))
        per_season = []
        for s in seasons:
            s_recs = [r for r in recs if r["season"] == s]
            agg         = aggregate_group(s_recs)
            agg["season"] = s
            per_season.append(agg)
        season_summary[resort] = per_season

    return {
        "weekly":          weekly,
        "monthly":         monthly,
        "seasonal":        seasonal,
        "daily_ranking":   daily_ranking,
        "powder_calendar": powder_calendar,
        "season_summary":  season_summary,
    }

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("Step 3 — Compute Derived Metrics")
    print("=" * 65)

    master = json.load(open(IN_PATH))
    records = master["records"]
    print(f"  Loaded {len(records)} records from master_data.json")

    # Enrich
    print("  Computing per-day scores, flags, keys ...")
    enriched = enrich(records)

    # Verify score range
    bad = [r for r in enriched if not (0 <= r["score_universal"] <= 1)]
    print(f"  Score range check: {len(bad)} out-of-range values (expect 0)")

    # Aggregations
    print("  Building weekly / monthly / yearly aggregations ...")
    aggs = build_aggregations(enriched)

    # ── Print sanity table ─────────────────────────────────────────────────
    # Derive season keys from the data so new seasons appear automatically
    SEASON_KEYS = sorted(set(r["season"] for r in enriched))
    print("\n  ── Seasonal avg universal score per resort ──")
    print(f"  {'Resort':<14}", end="")
    for s in SEASON_KEYS:
        print(f"  {s}", end="")
    print()
    print("  " + "-" * 74)
    for resort in RESORTS:
        print(f"  {resort:<14}", end="")
        for s in SEASON_KEYS:
            val = aggs["seasonal"][resort].get(s, {}).get("score_universal")
            print(f"  {val:.2f} " if val else "    —  ", end="")
        print()

    print("\n  ── Powder days per resort per season ──")
    print(f"  {'Resort':<14}", end="")
    for s in SEASON_KEYS:
        print(f"  {s}", end="")
    print()
    print("  " + "-" * 74)
    for resort in RESORTS:
        print(f"  {resort:<14}", end="")
        for s in SEASON_KEYS:
            val = aggs["seasonal"][resort].get(s, {}).get("powder_days", 0)
            print(f"  {val:>6}", end="")
        print()

    # ── Assemble output ────────────────────────────────────────────────────
    output = {
        "_meta": {
            **master["_meta"],
            "step": "3 — enriched + aggregated",
            "personas": list(PERSONAS.keys()),
            "score_fields": [f"score_{p}" for p in PERSONAS],
        },
        "records":      enriched,
        "aggregations": aggs,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"\n  enriched_data.json → {size_kb:.0f} KB")
    print("  Step 3 complete. Ready for Step 4 → HTML skeleton + themes.")

if __name__ == "__main__":
    main()
