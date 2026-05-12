import sys

mca_val = 193.05
linear_val = 65.69
savings = (1 - (linear_val / mca_val)) * 100

# SVG dimensions
width = 800
height = 500
padding = 80
chart_w = width - (padding * 2)
chart_h = height - (padding * 2)

# Scaling
max_val = 200
mca_h = (mca_val / max_val) * chart_h
linear_h = (linear_val / max_val) * chart_h

svg = f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
  <!-- Background -->
  <rect width="100%" height="100%" fill="#0f172a" rx="16"/>
  <defs>
    <linearGradient id="mcaGrad" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" style="stop-color:#3b82f6;stop-opacity:1" />
      <stop offset="100%" style="stop-color:#1d4ed8;stop-opacity:1" />
    </linearGradient>
    <linearGradient id="linearGrad" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" style="stop-color:#10b981;stop-opacity:1" />
      <stop offset="100%" style="stop-color:#059669;stop-opacity:1" />
    </linearGradient>
    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur in="SourceAlpha" stdDeviation="3" />
      <feOffset dx="0" dy="4" result="offsetblur" />
      <feComponentTransfer>
        <feFuncA type="linear" slope="0.5" />
      </feComponentTransfer>
      <feMerge>
        <feMergeNode />
        <feMergeNode in="SourceGraphic" />
      </feMerge>
    </filter>
  </defs>

  <!-- Title -->
  <text x="{width/2}" y="50" text-anchor="middle" fill="#f8fafc" font-family="sans-serif" font-size="28" font-weight="bold">
    Linear Benchmark: Disk Usage Comparison
  </text>

  <!-- Grid lines -->
  <line x1="{padding}" y1="{height-padding}" x2="{width-padding}" y2="{height-padding}" stroke="#334155" stroke-width="2"/>
  <line x1="{padding}" y1="{height-padding-chart_h}" x2="{width-padding}" y2="{height-padding-chart_h}" stroke="#1e293b" stroke-width="1"/>
  <line x1="{padding}" y1="{height-padding-chart_h/2}" x2="{width-padding}" y2="{height-padding-chart_h/2}" stroke="#1e293b" stroke-width="1"/>

  <!-- Y-Axis labels -->
  <text x="{padding-10}" y="{height-padding}" text-anchor="end" fill="#94a3b8" font-family="sans-serif" font-size="14">0 MB</text>
  <text x="{padding-10}" y="{height-padding-chart_h/2}" text-anchor="end" fill="#94a3b8" font-family="sans-serif" font-size="14">100 MB</text>
  <text x="{padding-10}" y="{height-padding-chart_h}" text-anchor="end" fill="#94a3b8" font-family="sans-serif" font-size="14">200 MB</text>

  <!-- MCA Bar -->
  <rect x="{padding + chart_w/4 - 60}" y="{height - padding - mca_h}" width="120" height="{mca_h}" fill="url(#mcaGrad)" rx="8" filter="url(#shadow)"/>
  <text x="{padding + chart_w/4}" y="{height - padding - mca_h - 15}" text-anchor="middle" fill="#f8fafc" font-family="sans-serif" font-size="20" font-weight="bold">{mca_val} MB</text>
  <text x="{padding + chart_w/4}" y="{height - padding + 25}" text-anchor="middle" fill="#94a3b8" font-family="sans-serif" font-size="16">MCA (Vanilla)</text>

  <!-- Linear Bar -->
  <rect x="{padding + 3*chart_w/4 - 60}" y="{height - padding - linear_h}" width="120" height="{linear_h}" fill="url(#linearGrad)" rx="8" filter="url(#shadow)"/>
  <text x="{padding + 3*chart_w/4}" y="{height - padding - linear_h - 15}" text-anchor="middle" fill="#f8fafc" font-family="sans-serif" font-size="20" font-weight="bold">{linear_val} MB</text>
  <text x="{padding + 3*chart_w/4}" y="{height - padding + 25}" text-anchor="middle" fill="#94a3b8" font-family="sans-serif" font-size="16">Linear (Compressed)</text>

  <!-- Savings Badge -->
  <circle cx="{width-padding-50}" cy="100" r="60" fill="none" stroke="#10b981" stroke-width="4" stroke-dasharray="8 4"/>
  <text x="{width-padding-50}" y="95" text-anchor="middle" fill="#10b981" font-family="sans-serif" font-size="24" font-weight="bold">{savings:.1f}%</text>
  <text x="{width-padding-50}" y="120" text-anchor="middle" fill="#10b981" font-family="sans-serif" font-size="12">SPACE SAVED</text>

</svg>
"""

with open("assets/benchmark_graph.svg", "w") as f:
    f.write(svg)

print("Generated assets/benchmark_graph.svg")
