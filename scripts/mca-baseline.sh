#!/usr/bin/env bash
# =============================================================================
# mca-baseline.sh
# Companion script for the MCA (vanilla) benchmark run.
#
# Run this ALONGSIDE a vanilla NeoForge server (no Linear mod).
# It tracks duration and final disk sizes, then writes a JSON report that
# matches the format produced by PregenExporter so compare.sh can diff them.
#
# Usage:
#   bash scripts/mca-baseline.sh <WORLD_FOLDER> <LOG_FILE>
#
# Example (Prism instance "Vanilla"):
#   bash scripts/mca-baseline.sh \
#     ~/.local/share/PrismLauncher/instances/Vanilla/minecraft/saves/NewWorld \
#     ~/.local/share/PrismLauncher/instances/Vanilla/minecraft/logs/latest.log
#
# The script exits (and writes the JSON) when it detects Chunky's
# "Generation complete" line in the log, or when you press Ctrl+C.
# =============================================================================

set -euo pipefail

WORLD_FOLDER="${1:-}"
LOG_FILE="${2:-}"

if [[ -z "$WORLD_FOLDER" || -z "$LOG_FILE" ]]; then
    echo "Usage: $0 <world_folder> <log_file>"
    exit 1
fi

if [[ ! -d "$WORLD_FOLDER" ]]; then
    echo "ERROR: world folder not found: $WORLD_FOLDER"
    exit 1
fi

TIMESTAMP=$(date -u +"%Y-%m-%d_%H-%M-%S")
OUT_FILE="${WORLD_FOLDER}/mca-baseline-${TIMESTAMP}.json"
START_EPOCH=$(date +%s)

echo "[mca-baseline] Watching $LOG_FILE"
echo "[mca-baseline] Ctrl+C to stop and write report at any time."
echo "[mca-baseline] Will auto-stop on Chunky 'Generation complete' line."

# ── Helper: count .mca files and sum their sizes ─────────────────────────────
scan_mca() {
    local folder="$1"
    find "$folder" -name "*.mca" -type f 2>/dev/null \
        | awk 'BEGIN{c=0;t=0} {c++; cmd="stat -c%s " $0; cmd | getline s; close(cmd); t+=s} END{print c " " t}'
}

# ── Wait for Chunky completion ────────────────────────────────────────────────
wait_for_completion() {
    # tail -F follows log rotations; grep exits 0 on first match
    tail -F "$LOG_FILE" 2>/dev/null | grep -m 1 -i "generation.*complete\|task.*finished\|chunky.*done" || true
}

cleanup_and_report() {
    local end_epoch
    end_epoch=$(date +%s)
    local duration=$(( end_epoch - START_EPOCH ))

    # Scan all dimension folders for .mca files
    read -r mca_count mca_bytes <<< "$(scan_mca "$WORLD_FOLDER")"

    local mca_mb
    mca_mb=$(echo "scale=2; ${mca_bytes:-0} / 1048576" | bc)

    echo ""
    echo "[mca-baseline] Pregen complete."
    echo "  Duration  : ${duration}s"
    echo "  .mca files: ${mca_count}"
    echo "  Total size: ${mca_mb} MB"
    echo "  Report    : $OUT_FILE"

    # Write JSON (matches PregenExporter output keys for compare.sh)
    cat > "$OUT_FILE" <<EOF
{
  "format": "mca-baseline",
  "version": "1.0",
  "trigger": "chunky-complete",
  "timestamp": "${TIMESTAMP}",
  "uptime_seconds": ${duration},

  "chunks": {
    "written": null,
    "read": null,
    "avg_write_ms": null,
    "avg_read_ms": null,
    "min_write_ms": null,
    "max_write_ms": null,
    "min_read_ms": null,
    "max_read_ms": null
  },

  "regions": {
    "flushes": null,
    "loads": null,
    "avg_flush_ms": null,
    "avg_load_ms": null,
    "flush_compress_ms_total": null,
    "flush_write_ms_total": null
  },

  "compression": {
    "uncompressed_bytes": null,
    "compressed_bytes": null,
    "ratio_pct_saved": 0.0
  },

  "cache": {
    "hits": null,
    "misses": null,
    "hit_rate": null
  },

  "disk": {
    "mca_file_count": ${mca_count},
    "mca_total_bytes": ${mca_bytes},
    "mca_total_mb": ${mca_mb}
  }
}
EOF
    echo "[mca-baseline] Done."
}

trap cleanup_and_report EXIT INT TERM

# Block until Chunky says it's done (or user Ctrl+C)
wait_for_completion

# Let cleanup_and_report fire via the EXIT trap
