#!/usr/bin/env python3
"""
action_refresh.py — GitHub Actions orchestrator
Handles graceful degradation if the APIs are down but cached data exists.
"""

import subprocess
import sys
import json
import argparse
from pathlib import Path

BASE    = Path(__file__).parent.parent
SCRIPTS = BASE / "scripts"
DATA    = BASE / "data" / "processed" / "enriched_data.json"
OUT     = BASE / "nauders_dashboard.html"

def run(cmd, desc, allow_fail=False):
    print(f"\n▶ {desc}")
    print(f"  CMD: {' '.join(cmd)}")
    sys.stdout.flush()  # force flush before subprocess captures stdout
    result = subprocess.run(cmd)
    sys.stdout.flush()
    if result.returncode != 0:
        if allow_fail:
            print(f"  ⚠ {desc} failed, but continuing gracefully.")
            sys.stdout.flush()
            return False
        else:
            print(f"  ✗ {desc} failed (exit {result.returncode}). Aborting.")
            sys.stdout.flush()
            sys.exit(result.returncode)
    return True

def last_baked_date():
    if not DATA.exists(): return None
    try:
        with open(DATA, "r") as f:
            d = json.load(f)
            records = d.get("records", [])
            # records sorted by (date, resort) — last entry has the latest date
            return records[-1]["date"] if records else None
    except Exception:
        return None

def _write_action_output(key, value):
    import os
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"{key}={value}\n")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--end-date", default=None)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    # 1. Fetch History
    fetch_cmd = [sys.executable, str(SCRIPTS / "fetch_openmeteo.py")]
    if args.end_date:
        fetch_cmd.extend(["--end-date", args.end_date])
    if args.force:
        fetch_cmd.append("--force")
        
    has_cache = DATA.exists()
    fetch_success = run(fetch_cmd, "Fetch Open-Meteo", allow_fail=has_cache)
    
    if not fetch_success and has_cache:
        print("\n[!] WARNING: Fetch failed, but cached data exists. Proceeding with stale data.")
    else:
        run([sys.executable, str(SCRIPTS / "clean_normalize.py")], "Clean & Normalize")
        run([sys.executable, str(SCRIPTS / "compute_metrics.py")], "Compute scores")

    # 2. Fetch Forecast (Phase 2)
    run([sys.executable, str(SCRIPTS / "fetch_forecast.py")], "Fetch High-Res Forecast", allow_fail=True)

    # 3. Build & Wrap
    run([sys.executable, str(BASE / "build_dashboard.py")], "Build HTML")

    og_script = SCRIPTS / "gen_og_image.py"
    if og_script.exists():
        run([sys.executable, str(og_script)], "Regenerate og-image.png", allow_fail=True)

    if not OUT.exists():
        sys.exit(1)

    new_baked = last_baked_date()
    # Only signal a change when we have real baked output.
    # The git diff in the workflow step is the authoritative empty-commit guard,
    # but this flag stops downstream steps from running needlessly on stale-cache runs.
    dashboard_changed = OUT.exists() and new_baked is not None
    _write_action_output("DASHBOARD_CHANGED", "true" if dashboard_changed else "false")
    _write_action_output("BAKED_THROUGH", new_baked or "unknown")

if __name__ == "__main__":
    main()