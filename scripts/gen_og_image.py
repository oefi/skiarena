#!/usr/bin/env python3
"""
Generate og-image.png (1200x630) for Zwei Laender Skiarena dashboard.
Rewritten for the HSL Bluebird Matrix aesthetic. Zero external JSON dependencies.
"""
import cairosvg, math, random
from pathlib import Path

OUT = Path(__file__).parent.parent / "og-image.png"

# Hardened Brutalist Palette
BG = "#0d1829"
S2_COL = "#1a2d4e"
TEXT = "#e8edf5"
MUTED = "#8fa0ba"
BRAND = "#3b82f6"

def get_color(s):
    """Matches the exact frontend HSL interpolation (0=Red, 120=Green)"""
    h = int(s * 120)
    return f"hsl({h}, 80%, 45%)"

def main():
    W, H = 1200, 630
    e = []
    
    def r(x, y, w, h, fill, rx=0, op=1.0):
        return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" rx="{rx}" fill-opacity="{op}"/>'
    
    def t(x, y, txt, size, fill, weight="normal", align="start"):
        return f'<text x="{x}" y="{y}" font-family="system-ui, -apple-system, sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{align}">{txt}</text>'

    # Base Canvas
    e.append(r(0, 0, W, H, BG))
    
    # Title Block
    e.append(t(80, 120, "🎿 ZL Skiarena", 64, TEXT, weight="800"))
    e.append(t(80, 180, "Historical Bluebird & Ski Telemetry", 32, MUTED, weight="600"))
    
    # Matrix Mockup Box
    box_x, box_y, box_w, box_h = 80, 240, 1040, 300
    e.append(r(box_x, box_y, box_w, box_h, S2_COL, rx=12))
    
    # Synthetic Heatmap Generation (Simulating 6 seasons of data)
    random.seed(42) # Deterministic renders
    cols = 40
    rows = 6
    cell_size = 20
    gap = 4
    
    grid_w = (cols * cell_size) + ((cols - 1) * gap)
    grid_h = (rows * cell_size) + ((rows - 1) * gap)
    
    start_x = box_x + (box_w - grid_w) / 2
    start_y = box_y + (box_h - grid_h) / 2
    
    for row in range(rows):
        e.append(t(start_x - 15, start_y + (row * (cell_size + gap)) + 15, f"20{20+row}/{21+row}", 14, MUTED, weight="bold", align="end"))
        
        for col in range(cols):
            # Generate a realistic sine-wave pattern for winter weather + noise
            base_score = 0.5 + (math.sin(col / 3.0) * 0.3)
            noise = random.uniform(-0.2, 0.2)
            score = max(0.0, min(1.0, base_score + noise))
            
            x = start_x + col * (cell_size + gap)
            y = start_y + row * (cell_size + gap)
            
            # 5% chance of absolute zero (Storm day)
            if random.random() < 0.05:
                score = 0.0
            
            e.append(r(x, y, cell_size, cell_size, get_color(score), rx=4))

    # Bottom Branding
    e.append(r(0, H-40, W, 40, "#060d18", op=0.9))
    e.append(t(80, H-14, "Open-Meteo ERA5-Land · Advanced Matrix Normalization", 16, MUTED, weight="bold"))
    e.append(t(W-80, H-14, "oefi.github.io/zlskiarena", 16, BRAND, weight="bold", align="end"))

    svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}">' + "".join(e) + '</svg>'
    
    print(f"Rendering OG Image to {OUT}...")
    cairosvg.svg2png(bytestring=svg.encode('utf-8'), write_to=str(OUT))
    print("✓ Success")

if __name__ == "__main__":
    main()
