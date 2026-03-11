#!/usr/bin/env python3
"""
build_dashboard.py — injects JSON data into dashboard_template.html
Output: nauders_dashboard.html (renamed to index.html in GitHub Actions)
"""
from pathlib import Path
import json, sys

BASE     = Path(__file__).parent
TMPL     = BASE / "dashboard_template.html"
DATA     = BASE / "data" / "processed" / "enriched_data.json"
FORECAST = BASE / "data" / "processed" / "forecast_data.json"
OUT      = BASE / "nauders_dashboard.html"

def main():
    print("Building dashboard…")
    template = TMPL.read_text(encoding="utf-8")
    
    if "__SKI_DATA_PLACEHOLDER__" not in template:
        sys.exit("ERROR: __SKI_DATA_PLACEHOLDER__ not found in template")

    # Inject History
    data_str = DATA.read_text(encoding="utf-8")
    html = template.replace("__SKI_DATA_PLACEHOLDER__", data_str)

    # Inject Forecast
    if "__FORECAST_DATA_PLACEHOLDER__" in html:
        if FORECAST.exists():
            fc_str = FORECAST.read_text(encoding="utf-8")
            html = html.replace("__FORECAST_DATA_PLACEHOLDER__", fc_str)
            print("  ✓ Injected high-res forecast data")
        else:
            print("  ⚠ No forecast data found, injecting empty object.")
            html = html.replace("__FORECAST_DATA_PLACEHOLDER__", '{"error": "no forecast generated"}')

    OUT.write_text(html, encoding="utf-8")

    size_kb = OUT.stat().st_size / 1024
    print(f"  → {OUT.name}  ({size_kb:.0f} KB)")
    print("Done.")

if __name__ == "__main__":
    main()