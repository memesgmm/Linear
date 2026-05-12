#!/usr/bin/env bash
# =============================================================================
# compare.sh
# Reads an mca-baseline-*.json and a linear-pregen-stats-*.json and prints
# a side-by-side comparison table.
#
# Usage:
#   bash scripts/compare.sh <mca-baseline.json> <linear-pregen-stats.json>
#
# Dependencies: jq (install with: sudo apt install jq)
# =============================================================================

set -euo pipefail

MCA_JSON="${1:-}"
LINEAR_JSON="${2:-}"

if [[ -z "$MCA_JSON" || -z "$LINEAR_JSON" ]]; then
    echo "Usage: $0 <mca-baseline.json> <linear-pregen-stats.json>"
    exit 1
fi

if ! command -v jq &>/dev/null; then
    echo "ERROR: jq is required. Install with: sudo apt install jq"
    exit 1
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
jq_str()  { jq -r "${2}" "${1}" 2>/dev/null || echo "n/a"; }
jq_num()  { jq -r "${2} // \"n/a\"" "${1}" 2>/dev/null; }

fmt_mb()  {
    local bytes="$1"
    if [[ "$bytes" == "null" || "$bytes" == "n/a" ]]; then echo "n/a"; return; fi
    echo "scale=2; $bytes / 1048576" | bc
}

pct_savings() {
    local mca="$1" lin="$2"
    if [[ "$mca" == "null" || "$mca" == "n/a" || "$lin" == "null" || "$lin" == "n/a" || "$mca" == "0" ]]; then
        echo "n/a"
        return
    fi
    echo "scale=1; (1 - ($lin / $mca)) * 100" | bc
}

speedup() {
    local mca_s="$1" lin_s="$2"
    if [[ "$mca_s" == "null" || "$mca_s" == "n/a" || "$lin_s" == "null" || "$lin_s" == "n/a" || "$lin_s" == "0" ]]; then
        echo "n/a"
        return
    fi
    echo "scale=1; $mca_s / $lin_s" | bc
}

# ── Read values ───────────────────────────────────────────────────────────────
mca_ts=$(jq_str "$MCA_JSON" ".timestamp")
lin_ts=$(jq_str "$LINEAR_JSON" ".timestamp")

mca_uptime=$(jq_num "$MCA_JSON" ".uptime_seconds")
lin_uptime=$(jq_num "$LINEAR_JSON" ".uptime_seconds")

mca_disk_files=$(jq_num "$MCA_JSON"    ".disk.mca_file_count")
lin_disk_files=$(jq_num "$LINEAR_JSON" ".disk.linear_file_count")

mca_disk_bytes=$(jq_num "$MCA_JSON"    ".disk.mca_total_bytes")
lin_disk_bytes=$(jq_num "$LINEAR_JSON" ".disk.linear_total_bytes")

mca_disk_mb=$(fmt_mb "$mca_disk_bytes")
lin_disk_mb=$(fmt_mb "$lin_disk_bytes")

lin_chunks_written=$(jq_num "$LINEAR_JSON" ".chunks.written")
lin_avg_write_ms=$(jq_num "$LINEAR_JSON"   ".chunks.avg_write_ms")
lin_min_write_ms=$(jq_num "$LINEAR_JSON"   ".chunks.min_write_ms")
lin_max_write_ms=$(jq_num "$LINEAR_JSON"   ".chunks.max_write_ms")

lin_flushes=$(jq_num "$LINEAR_JSON"       ".regions.flushes")
lin_avg_flush_ms=$(jq_num "$LINEAR_JSON"  ".regions.avg_flush_ms")

lin_uncomp=$(jq_num "$LINEAR_JSON"   ".compression.uncompressed_bytes")
lin_comp=$(jq_num "$LINEAR_JSON"     ".compression.compressed_bytes")
lin_ratio=$(jq_num "$LINEAR_JSON"    ".compression.ratio_pct_saved")

lin_cache_hits=$(jq_num "$LINEAR_JSON"   ".cache.hits")
lin_cache_misses=$(jq_num "$LINEAR_JSON" ".cache.misses")
lin_hit_rate=$(jq_num "$LINEAR_JSON"     ".cache.hit_rate")

# Derived
disk_savings=$(pct_savings "$mca_disk_bytes" "$lin_disk_bytes")
time_speedup=$(speedup "$mca_uptime" "$lin_uptime")

# ── Output ────────────────────────────────────────────────────────────────────
BOLD=$'\e[1m'; RESET=$'\e[0m'; GREEN=$'\e[32m'; CYAN=$'\e[36m'

echo ""
echo "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo "${BOLD}║         LinearReader — Pregen Benchmark Comparison           ║${RESET}"
echo "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""
printf "  %-30s %-20s %-20s\n" "Metric" "MCA (baseline)" "Linear"
printf "  %-30s %-20s %-20s\n" "──────────────────────────────" "────────────────────" "────────────────────"

printf "  %-30s %-20s %-20s\n" "Timestamp" "$mca_ts" "$lin_ts"
printf "  %-30s %-20s %-20s\n" "Pregen duration (s)" "${mca_uptime}s" "${lin_uptime}s"
printf "  %-30s %-20s %-20s  ${GREEN}← ${time_speedup}x faster${RESET}\n" "Region files" "$mca_disk_files" "$lin_disk_files"
printf "  %-30s %-20s %-20s  ${GREEN}← ${disk_savings}%% smaller${RESET}\n" "Total disk usage (MB)" "${mca_disk_mb} MB" "${lin_disk_mb} MB"
echo ""
echo "${CYAN}  ── Linear internal stats ──${RESET}"
printf "  %-30s %-20s\n" "Chunks written"        "$lin_chunks_written"
printf "  %-30s %-20s\n" "Avg chunk write (ms)"  "$lin_avg_write_ms"
printf "  %-30s %-20s\n" "Min chunk write (ms)"  "$lin_min_write_ms"
printf "  %-30s %-20s\n" "Max chunk write (ms)"  "$lin_max_write_ms"
printf "  %-30s %-20s\n" "Region flushes"        "$lin_flushes"
printf "  %-30s %-20s\n" "Avg flush (ms)"        "$lin_avg_flush_ms"
printf "  %-30s %-20s\n" "Uncompressed (bytes)"  "$lin_uncomp"
printf "  %-30s %-20s\n" "Compressed   (bytes)"  "$lin_comp"
printf "  %-30s %-20s\n" "Compression saving"    "${lin_ratio}%"
printf "  %-30s %-20s\n" "Cache hits/misses"     "${lin_cache_hits}/${lin_cache_misses}"
printf "  %-30s %-20s\n" "Cache hit rate"        "$lin_hit_rate"
echo ""
echo "${BOLD}  Summary:${RESET}"
echo "    Disk savings : ${GREEN}${disk_savings}%${RESET}"
echo "    Time speedup : ${GREEN}${time_speedup}x${RESET}"
echo ""
