import sys

mca_val = 193.05
linear_val = 65.69
savings = (1 - (linear_val / mca_val)) * 100

# SVG dimensions
width = 600
height = 200
bar_h = 30
gap = 40
padding = 40

# Scaling (max width is 400)
max_w = 400
mca_w = (mca_val / mca_val) * max_w
linear_w = (linear_val / mca_val) * max_w

svg = f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#ffffff"/>
  
  <!-- MCA Bar -->
  <text x="{padding}" y="50" fill="#64748b" font-family="sans-serif" font-size="12" font-weight="600">ANVIL (BASELINE)</text>
  <rect x="{padding}" y="60" width="{mca_w}" height="{bar_h}" fill="#e2e8f0" rx="4"/>
  <text x="{padding + mca_w + 10}" y="80" fill="#1e293b" font-family="sans-serif" font-size="14" font-weight="bold">{mca_val} MB</text>

  <!-- Linear Bar -->
  <text x="{padding}" y="120" fill="#0ea5e9" font-family="sans-serif" font-size="12" font-weight="600">LINEAR</text>
  <rect x="{padding}" y="130" width="{linear_w}" height="{bar_h}" fill="#0ea5e9" rx="4"/>
  <text x="{padding + linear_w + 10}" y="150" fill="#0ea5e9" font-family="sans-serif" font-size="14" font-weight="bold">{linear_val} MB</text>

  <!-- Savings Label -->
  <text x="{width - padding}" y="150" text-anchor="end" fill="#0ea5e9" font-family="sans-serif" font-size="24" font-weight="bold">-{savings:.0f}%</text>
</svg>
"""

with open("assets/benchmark_graph.svg", "w") as f:
    f.write(svg)

print("Generated assets/benchmark_graph.svg")
