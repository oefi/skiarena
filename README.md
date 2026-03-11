### Zwei Länder Skiarena — Ski Conditions Dashboard

Historical ski weather for all 5 resorts of the **Zwei Länder Skiarena** (Austria / South Tyrol).  
Dec–Apr · Seasons 2019/20–2024/25 · plus live 2025/26 fetched in-browser.

### Demo: https://oefi.github.io/zlsa/

---

### Resorts

| Resort | Country | Base | Summit |
|---|---|---|---|
| Nauders am Reschenpass | 🇦🇹 North Tyrol | 1,400 m | 2,750 m |
| Schöneben–Haideralm | 🇮🇹 South Tyrol | 1,460 m | 2,390 m |
| Watles | 🇮🇹 South Tyrol | 1,500 m | 2,550 m |
| Sulden am Ortler | 🇮🇹 South Tyrol | 1,900 m | 3,250 m |
| Trafoi am Ortler | 🇮🇹 South Tyrol | 1,540 m | 2,800 m |

### What's inside

- **Best Weeks heatmap** — colour-coded weekly ski quality score, all 6 seasons at a glance
- **Daily drill-down** — all 5 resorts ranked for any day; powder / sunshine / lift-risk badges
- **Snow depth ribbons** — season-over-season snowpack chart with toggleable layers
- **Powder day counter** — grouped bar chart with month and threshold filters
- **Monthly trends** — score trajectory Dec → Apr across seasons
- **Plan My Week** — pick any 7–14 night window, get a day-by-day resort recommendation
- **Live 2025/26 data** — Open-Meteo ERA5-Land fetched on page load, cached 24 h

### Ski Quality Score

Daily score 0–1 computed from summit snow depth, fresh snow, summit temperature, sunshine hours, and wind speed. Four persona presets re-weight the components:

| Persona | Snow | Temp | Sun | Wind |
|---|---|---|---|---|
| 🎿 Universal | 35% | 25% | 25% | 15% |
| ❄️ Powder | 55% | 25% | 10% | 10% |
| ☀️ Sun | 20% | 20% | 50% | 10% |
| 👨‍👩‍👧 Family | 25% | 30% | 35% | 10% |

### Data

Baked data: Open-Meteo ERA5-Land reanalysis · Dec 10 → Apr 10 · 6 complete seasons.  
Current season (2025/26): fetched live from `archive-api.open-meteo.com` on page load.

> **Note:** Data in this repo is currently **synthetic** (generated from published climate norms for development). Run `scripts/fetch_openmeteo.py` to replace with real ERA5-Land data, then rebuild.

### Rebuild

```bash
# Generate synthetic data (development)
python3 scripts/generate_synthetic.py

# Or fetch real Open-Meteo data
pip install requests
python3 scripts/fetch_openmeteo.py

# Process
python3 scripts/clean_normalize.py
python3 scripts/compute_metrics.py

# Build single-file dashboard
python3 build_dashboard.py
# → nauders_dashboard.html  (~2.9 MB)
```
## Caveats

- ERA5-Land is model reanalysis, not station observations. Micro-valley effects may be missed.
- 6 seasons is recent history, not long-term climatology.
- Snowmaking coverage is not reflected in model data.
- Ski 6 surcharge applies at Sulden and Trafoi.
