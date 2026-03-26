#!/usr/bin/env python3
"""Generate og-image.png (1200x630) for Zwei Laender Skiarena dashboard.
Heatmap data is loaded from enriched_data.json (real Bluebird Scores).
Falls back to synthetic curves only if the enriched file is missing.
"""
import cairosvg, math, json
from pathlib import Path
from collections import defaultdict
from calendar import isleap

OUT          = Path(__file__).parent.parent / "og-image.png"
ENRICHED     = Path(__file__).parent.parent / "data" / "processed" / "enriched_data.json"

# Palette
BG="#0d1829"; S2_COL="#1a2d4e"; ACCENT="#4d7fff"; ACCENT2="#7c9dff"
TEXT="#e8edf5"; MUTED="#8fa0ba"; FAINT="#4a5a72"; BORDER="#1e3260"
S5="#22c55e"; S4="#84cc16"; S3="#eab308"; S2="#f97316"; S1="#ef4444"

def sc(s):
    if s >= 0.85: return S5
    if s >= 0.70: return S4
    if s >= 0.55: return S3
    if s >= 0.40: return S2
    return S1


# ── Load real Bluebird Scores from enriched_data.json ────────────────────────
# Each season row in the heatmap = 18 weekly buckets (Dec wk1 → Apr wk4 roughly).
# We compute the mean score across all 5 resorts per calendar-week bucket.
# Falls back to a synthetic bell curve only if the data file doesn't exist.

def _load_real_heatmap():
    """Return (SEASONS list, HM dict {season: [18 floats]}) from enriched_data."""
    if not ENRICHED.exists():
        return None, None
    try:
        with open(ENRICHED) as f:
            d = json.load(f)
        records = d.get("records", [])
        if not records:
            return None, None

        # Bucket records by season and ISO week-within-season
        # Season window: Dec 1 – Apr 30 mapped to 18 weekly slots (≈4.33d/slot)
        # Slot = floor((day_of_season - 0) / 18) capped to [0, 17]
        from datetime import date as _date
        bucket_scores = defaultdict(lambda: defaultdict(list))

        for r in records:
            score = r.get("score")
            if score is None:
                continue
            ds = r["date"]
            y, m, day = int(ds[:4]), int(ds[5:7]), int(ds[8:10])
            # Season start year
            sy = y if m >= 11 else y - 1
            season_label = f"{sy}/{str(sy+1)[2:]}"
            # Day offset within season: Nov 1 = 0
            season_start = _date(sy, 11, 1)
            current     = _date(y, m, day)
            off = (current - season_start).days
            # Map season to 18 slots; show Dec–Apr only (skip Nov = days 0–29).
            # May 1 offset = 181 in normal years, 182 in leap years (Feb has 29 days).
            # The hardcoded 181 silently excluded May 1 data in every leap-year season
            # (2019/20, 2023/24, 2027/28 …). Fixed: compute bound from calendar.
            _max_off = 182 if isleap(sy + 1) else 181
            if off < 30 or off > _max_off:
                continue
            slot = min(17, (off - 30) * 18 // 152)
            bucket_scores[season_label][slot].append(score)

        if not bucket_scores:
            return None, None

        seasons = sorted(bucket_scores.keys())
        hm = {}
        for s in seasons:
            row = []
            for slot in range(18):
                vals = bucket_scores[s].get(slot, [])
                row.append(sum(vals) / len(vals) if vals else 0.5)
            hm[s] = row
        return seasons, hm
    except Exception as e:
        print(f"  [!] OG image: could not load enriched_data: {e}. Using synthetic.")
        return None, None


def _synthetic_heatmap():
    """Reproducible synthetic bell curve fallback."""
    import random as _random
    _random.seed(42)
    seasons = ["2019/20","2020/21","2021/22","2022/23","2023/24","2024/25"]
    smods   = {"2019/20":+.05,"2020/21":-.08,"2021/22":+.10,
               "2022/23":-.03,"2023/24":+.07,"2024/25":+.02}
    def wb(wn): return .35 + .55 * math.exp(-((wn-9)**2) / 50)
    return seasons, {s: [max(.05, min(1., wb(w)+smods[s]+_random.gauss(0,.07)))
                         for w in range(1, 19)] for s in seasons}


SEASONS, HM = _load_real_heatmap()
if SEASONS is None:
    SEASONS, HM = _synthetic_heatmap()

# Expected full-dataset record count: 5 resorts × ski-season days 2019/20–2025/26.
# Used as a fallback label on the OG image when enriched_data.json is absent or
# the real fetch failed and the file contains synthetic data with 0 real records.
EXPECTED_RECORDS = 6345  # 5 resorts × 1269 ski-season days

# Dynamic record count + date range from meta
_total_records = 0
_date_range    = "Nov–Apr · 2019/20–2024/25"
if ENRICHED.exists():
    try:
        with open(ENRICHED) as f:
            _meta = json.load(f).get("_meta", {})
        _total_records = _meta.get("total_records", 0)
        resorts_meta = _meta.get("resorts", [])
    except Exception:
        pass

W, H = 1200, 630
SERIF = "'DejaVu Serif','Georgia',serif"
SANS  = "'DejaVu Sans','Liberation Sans','Arial',sans-serif"
MONO  = "'DejaVu Sans Mono','Courier New',monospace"

def esc(s): return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def r(x,y,w,h,fill,rx=0,op=1,st=None,sw=1):
    s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}"'
    if rx: s += f' rx="{rx}"'
    if op < 1: s += f' opacity="{op}"'
    if st: s += f' stroke="{st}" stroke-width="{sw}"'
    return s + '/>'

def cir(cx,cy,radius,fill,op=1):
    return f'<circle cx="{cx}" cy="{cy}" r="{radius}" fill="{fill}"{"" if op==1 else f" opacity={op}"}/>'

def t(x,y,s,sz,fill,w="normal",a="start",f=SANS,it=False,op=1,ls=0):
    st = [f"font-size:{sz}px",f"font-weight:{w}",f"font-family:{f}",
          f"fill:{fill}",f"text-anchor:{a}"]
    if it: st.append("font-style:italic")
    if op < 1: st.append(f"opacity:{op}")
    if ls: st.append(f"letter-spacing:{ls}em")
    return (f'<text x="{x}" y="{y}" style="{";".join(st)}"'
            f' dominant-baseline="auto">{esc(s)}</text>')

def ln(x1,y1,x2,y2,stroke=BORDER,w=1,op=1):
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}"'
            f' stroke="{stroke}" stroke-width="{w}" opacity="{op}"/>')

def poly(pts,fill,op=1):
    return f'<polygon points="{pts}" fill="{fill}" opacity="{op}"/>'

def diamond_svg(cx, cy, rx, ry, fill, op=1):
    """Native SVG diamond — no font dependency."""
    pts = f"{cx},{cy-ry} {cx+rx},{cy} {cx},{cy+ry} {cx-rx},{cy}"
    return f'<polygon points="{pts}" fill="{fill}" opacity="{op}"/>'

# ─── Build SVG ────────────────────────────────────────────────────────────────
e = []

e.append(f'<rect width="{W}" height="{H}" fill="{BG}"/>')
e.append(f'''<defs>
<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
  <stop offset="0%"   stop-color="#1e3260" stop-opacity="0.40"/>
  <stop offset="100%" stop-color="#060d18" stop-opacity="0.70"/>
</linearGradient>
<linearGradient id="ac" x1="0" y1="0" x2="1" y2="0">
  <stop offset="0%"   stop-color="{ACCENT}"/>
  <stop offset="100%" stop-color="#6b8fff"/>
</linearGradient>
<linearGradient id="mtn" x1="0" y1="0" x2="0" y2="1">
  <stop offset="0%"   stop-color="#2a4080" stop-opacity="0.20"/>
  <stop offset="100%" stop-color="{BG}"    stop-opacity="0.00"/>
</linearGradient>
</defs>''')

e.append(r(0,0,W,H,"url(#bg)"))

# Mountain silhouette — lower-left, behind resort list
e.append(poly("54,495 115,388 170,428 226,344 288,436 346,360 398,414 448,375 500,430 548,380 596,434 632,400 648,475 648,502",
              "url(#mtn)"))
e.append(poly("54,522 94,458 135,478 180,405 230,450 275,400 327,445 374,415 420,450 464,420 514,454 564,424 612,458 648,435 648,522",
              "#0f1e38", op=0.45))
for px,py in [(226,344),(346,360),(448,375),(548,380)]:
    e.append(poly(f"{px-15},{py+14} {px},{py} {px+15},{py+14}", "#c8d8f0", op=0.14))

# Top accent bar
e.append(r(0,0,W,5,"url(#ac)"))
# Panel divider
e.append(ln(648,28,648,H-28, BORDER, 1, 0.6))

LX = 54

# ── Diamond accent mark (native SVG — no font) ──
e.append(diamond_svg(LX+14, 70, 13, 18, ACCENT2, op=0.92))

# Title
e.append(t(LX+36, 87,  "Zwei L\u00e4nder", 40, TEXT, w="300", f=SERIF, it=True))
e.append(t(LX+36, 136, "Skiarena",         54, TEXT, w="700", f=SERIF))
e.append(t(LX,    174, "Ski  Conditions  Dashboard", 17, MUTED, f=SANS, ls=0.10))
e.append(ln(LX, 193, 608, 193))

# Season + record chips
_seasons_str = f"Nov–Apr · {SEASONS[0]}–{SEASONS[-1]}" if SEASONS else "Nov–Apr · 2019/20–2024/25"
_rec_str     = f"{_total_records:,} records" if _total_records else f"~{EXPECTED_RECORDS:,} records"
e.append(r(LX, 206, 216, 30, S2_COL, rx=6))
e.append(t(LX+11, 226, _seasons_str, 12, ACCENT2, w="600", f=MONO))
e.append(r(LX+228, 206, 122, 30, S2_COL, rx=6))
e.append(t(LX+239, 226, _rec_str, 12, MUTED, f=MONO))

# Resort list
RESORTS = [
    ("AT","Nauders am Reschenpass",   "1,400\u20132,750 m"),
    ("IT","Sch\u00f6neben\u2013Haideralm","1,460\u20132,390 m"),
    ("IT","Watles",                   "1,500\u20132,550 m"),
    ("IT","Sulden am Ortler",         "1,900\u20133,250 m"),
    ("IT","Trafoi am Ortler",         "1,540\u20132,800 m"),
]
ry = 272
for ctry, name, elev in RESORTS:
    dc = "#ef4444" if ctry == "AT" else "#22c55e"
    e.append(r(LX-6, ry-17, 572, 28, S2_COL, rx=4, op=0.38))
    e.append(cir(LX+7, ry-4, 5, dc))
    e.append(t(LX+17, ry, ctry, 10, dc, w="700", f=MONO))
    e.append(t(LX+39, ry, name, 14, TEXT, f=SANS))
    e.append(t(600, ry, elev, 11, FAINT, f=MONO, a="end"))
    ry += 32

# ── Bottom left: feature pills + score legend ──────────────────────────────────
# Separator after resort list (last row ends at ~432)
e.append(ln(LX, 442, 608, 442, BORDER, 1, 0.35))
# Feature pills
fy = 453
ftags = [("Powder day counter", ACCENT),
         ("Monthly trends",     S3),
         ("Plan My Week",       S5)]
fx = LX
for label, col in ftags:
    w_pill = len(label) * 7 + 28
    e.append(r(fx, fy, w_pill, 22, S2_COL, rx=11, op=0.7))
    e.append(cir(fx+12, fy+11, 4, col))
    e.append(t(fx+21, fy+15, label, 11, MUTED, f=SANS))
    fx += w_pill + 10

# Score legend: y=500 (score swatch row)
ly = 500
e.append(t(LX, ly, "Score:", 11, FAINT, f=SANS))
lx = LX + 52
for sv, lab in [(.92,"Excellent"), (.77,"Good"), (.62,"OK"), (.47,"Fair"), (.22,"Poor")]:
    e.append(r(lx, ly-11, 11, 11, sc(sv), rx=2))
    e.append(t(lx+14, ly, lab, 11, MUTED, f=SANS))
    lx += 80

# ─── Heatmap (right panel) ────────────────────────────────────────────────────
# Right panel: x=648→1200 (552px). LBW=56, cell math:
# 18 * (CW+CG) - CG = GW;  GW = 465 → fits in 552-56-31 = 465. ✓
RX0=662; RY0=44; LBW=56; CW=23; CH=28; CG=3
GW = 18*(CW+CG) - CG   # 465
gx = RX0 + LBW
gy = RY0 + 52

e.append(t(RX0, RY0,    "Best Weeks Heatmap", 13, MUTED, w="600", f=SANS, ls=0.06))
e.append(t(RX0, RY0+17, "Weekly ski quality score \u00b7 all 5 resorts", 11, FAINT, f=SANS))

# Month markers
for wi, lab in {0:"Dec", 3:"Jan", 8:"Feb", 12:"Mar", 16:"Apr"}.items():
    mx = gx + wi*(CW+CG)
    e.append(t(mx, gy-8, lab, 10, ACCENT2, w="600", f=MONO))
    e.append(ln(mx, gy-4, mx, gy, ACCENT2, 1, 0.4))

GH = len(SEASONS)*(CH+CG) - CG
for si, season in enumerate(SEASONS):
    sy = gy + si*(CH+CG)
    e.append(t(gx-5, sy+CH-9, season, 9, MUTED, f=MONO, a="end"))
    for wi, score in enumerate(HM[season]):
        cx = gx + wi*(CW+CG)
        op = round(0.55 + score*0.43, 2)
        e.append(r(cx, sy, CW, CH, sc(score), rx=3, op=op))

e.append(r(gx-1, gy-1, GW+2, GH+2, "none", st=BORDER, sw=1))

# Annotation + season quality bars below grid
ay = gy + GH + 16
e.append(t(gx, ay+10, "\u25b8  Peak: Jan\u2013Feb  \u00b7  Dec builds  \u00b7  Apr fades",
           10, FAINT, f=SANS))

# Mini season-total bars (visual summary below heatmap)
bar_y = ay + 28
bar_h = 10
bar_max = 200  # max bar width in px
season_avgs = {s: sum(HM[s])/len(HM[s]) for s in SEASONS}
max_avg = max(season_avgs.values())

for si, season in enumerate(SEASONS):
    bx = gx
    by = bar_y + si * (bar_h + 4)
    avg = season_avgs[season]
    bw  = round((avg / max_avg) * bar_max)
    e.append(r(bx, by, bar_max, bar_h, S2_COL, rx=2, op=0.5))
    e.append(r(bx, by, bw, bar_h, sc(avg), rx=2, op=0.8))
    e.append(t(bx-5, by+bar_h-1, season, 8, FAINT, f=MONO, a="end"))
    e.append(t(bx+bar_max+5, by+bar_h-1,
               f"{avg:.0%}", 8, MUTED, f=MONO))

# ─── Bottom bar ───────────────────────────────────────────────────────────────
e.append(ln(0, H-26, W, H-26, BORDER, 1))
e.append(r(0, H-25, W, 25, "#060d18", op=0.9))
e.append(t(LX, H-8,
           "Open-Meteo ERA5 + ERA5-Land  \u00b7  Bluebird Score  \u00b7  Free & open-source",
           11, FAINT, f=SANS))
e.append(t(W-LX, H-8,
           "github.com/YOURUSERNAME/zweilaender-ski-dashboard",
           11, FAINT, f=SANS, a="end"))

# ─── Render ───────────────────────────────────────────────────────────────────
svg = (f'<?xml version="1.0" encoding="UTF-8"?>\n'
       f'<svg xmlns="http://www.w3.org/2000/svg"'
       f' width="{W}" height="{H}" viewBox="0 0 {W} {H}">\n'
       + "".join(e) + "\n</svg>")

cairosvg.svg2png(bytestring=svg.encode("utf-8"),
                 write_to=str(OUT),
                 output_width=W, output_height=H)
print(f"og-image.png  {OUT.stat().st_size//1024} KB  {W}x{H}px")
