#!/usr/bin/env python3
"""
action_refresh.py — GitHub Actions orchestrator for the ski dashboard.

Run order:
  1. fetch_openmeteo.py   — pull ERA5-Land data up to yesterday (or --end-date)
  2. clean_normalize.py   — merge base + summit, compute season/week keys
  3. compute_metrics.py   — enrich records, build aggregations
  4. build_dashboard.py   — inject JSON into HTML template

Exits 0 on success (Action commits the output).
Exits 1 if anything goes wrong (Action fails visibly, no silent corruption).

Also handles the off-season no-op: if today is Apr 11 – Dec 9 and the baked
data is already current through last Apr 10, the script exits 0 without
fetching or rebuilding, so the weekly cron just passes cleanly.

Usage:
  python3 scripts/action_refresh.py [--end-date YYYY-MM-DD] [--force]
"""

import subprocess
import sys
import json
import argparse
from pathlib import Path
from datetime import date, timedelta

BASE    = Path(__file__).parent.parent
SCRIPTS = BASE / "scripts"
DATA    = BASE / "data" / "processed" / "enriched_data.json"
OUT     = BASE / "nauders_dashboard.html"

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Refresh Ski Dashboard data + HTML")
    p.add_argument("--end-date", default=None,
                   help="Override fetch end date (YYYY-MM-DD)")
    p.add_argument("--force", action="store_true",
                   help="Run even if data appears current (skips no-op check)")
    return p.parse_args()

# ── Season helpers ────────────────────────────────────────────────────────────

def in_season(today=None):
    today = today or date.today()
    m, d = today.month, today.day
    return (m == 12 and d >= 10) or (1 <= m <= 3) or (m == 4 and d <= 10)

def current_season_start(today=None):
    today = today or date.today()
    if today.month == 12:
        return date(today.year, 12, 10)
    return date(today.year - 1, 12, 10)

def last_baked_date():
    """Read the latest date from the already-built enriched JSON, if it exists."""
    if not DATA.exists():
        return None
    try:
        data = json.loads(DATA.read_text())
        recs = data.get("records", [])
        return recs[-1]["date"] if recs else None
    except Exception:
        return None

# ── No-op check ───────────────────────────────────────────────────────────────

def should_skip(end_date_str, force):
    """
    Return (True, reason) if we can safely skip the rebuild,
    (False, None) if we must run.
    """
    if force:
        return False, None

    if not DATA.exists() or not OUT.exists():
        return False, None

    today   = date.today()
    baked   = last_baked_date()
    if not baked:
        return False, None

    baked_d = date.fromisoformat(baked)

    # Off-season: baked data is from last Apr 10 → nothing to add until Dec 10
    if not in_season(today):
        last_apr10 = date(today.year, 4, 10) if today > date(today.year, 4, 10) \
                     else date(today.year - 1, 4, 10)
        if baked_d >= last_apr10:
            return True, f"off-season and data is current through {baked}"

    # In-season: if baked is only 1-2 days behind (ERA5 lag) we might be current
    effective_end = date.fromisoformat(end_date_str) if end_date_str \
                    else date.today() - timedelta(days=2)
    if baked_d >= effective_end:
        return True, f"data already current through {baked}"

    return False, None

# ── Runner ────────────────────────────────────────────────────────────────────

def run(cmd, description):
    print(f"\n{'─'*60}")
    print(f"▶ {description}")
    print(f"  $ {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"\n✗ FAILED: {description} (exit {result.returncode})", file=sys.stderr)
        sys.exit(1)
    print(f"  ✓ done")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    print("=" * 60)
    print("🎿 Zwei Länder Skiarena — Dashboard Refresh")
    print(f"   date: {date.today().isoformat()}")
    print(f"   in-season: {in_season()}")
    if args.end_date:
        print(f"   end-date override: {args.end_date}")
    print("=" * 60)

    # ── No-op guard ──────────────────────────────────────────────────────
    skip, reason = should_skip(args.end_date, args.force)
    if skip:
        print(f"\n⏭  Skipping — {reason}")
        print("   No commit needed.")
        # Signal to the Action that there's nothing to commit
        _write_action_output("DASHBOARD_CHANGED", "false")
        sys.exit(0)

    # ── Step 1: Fetch ────────────────────────────────────────────────────
    fetch_cmd = [sys.executable, str(SCRIPTS / "fetch_openmeteo.py")]
    if args.end_date:
        fetch_cmd += ["--end-date", args.end_date]
    run(fetch_cmd, "Fetch Open-Meteo ERA5-Land data")

    # ── Step 2: Clean + normalise ────────────────────────────────────────
    run([sys.executable, str(SCRIPTS / "clean_normalize.py")],
        "Clean, normalize & merge")

    # ── Step 3: Enrich + aggregate ───────────────────────────────────────
    run([sys.executable, str(SCRIPTS / "compute_metrics.py")],
        "Compute scores & aggregations")

    # ── Step 4: Build HTML ───────────────────────────────────────────────
    run([sys.executable, str(BASE / "build_dashboard.py")],
        "Build single-file dashboard HTML")

    # ── Post-build check ─────────────────────────────────────────────────
    if not OUT.exists():
        print("\n✗ Output HTML not found after build.", file=sys.stderr)
        sys.exit(1)

    size_kb = OUT.stat().st_size / 1024
    new_baked = last_baked_date()
    print(f"\n{'='*60}")
    print(f"✓ Build complete")
    print(f"  Output : {OUT.name}  ({size_kb:.0f} KB)")
    print(f"  Baked through: {new_baked}")
    print(f"{'='*60}")

    _write_action_output("DASHBOARD_CHANGED", "true")
    _write_action_output("BAKED_THROUGH", new_baked or "unknown")

def _write_action_output(key, value):
    """Write to $GITHUB_OUTPUT if running in Actions, else just print."""
    import os
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"{key}={value}\n")
    else:
        print(f"  [output] {key}={value}")

if __name__ == "__main__":
    main()
