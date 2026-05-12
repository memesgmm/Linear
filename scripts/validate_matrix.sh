#!/bin/bash

# Linear Multi-Version Validation Matrix
# Tests compilation and unit tests against all supported 1.21.x versions.

set -e

VERSIONS=(
    "1.21.1:21.1.228"
    "1.21.2:21.2.1-beta"
    "1.21.3:21.3.56"
    "1.21.4:21.4.1"
    "1.21.5:21.5.1"
    "1.21.6:21.6.1"
    "1.21.7:21.7.1"
    "1.21.8:21.8.1"
    "1.21.9:21.9.1"
    "1.21.10:21.10.1"
    "1.21.11:21.11.1"
)

REPORT="matrix_report.md"
echo "# Version Validation Matrix Report" > $REPORT
echo "| Minecraft | NeoForge | Status | Result |" >> $REPORT
echo "| :--- | :--- | :--- | :--- |" >> $REPORT

for ENTRY in "${VERSIONS[@]}"; do
    MC_VER="${ENTRY%%:*}"
    NEO_VER="${ENTRY#*:}"
    
    echo "----------------------------------------------------"
    echo "VALIDATING: Minecraft $MC_VER (NeoForge $NEO_VER)"
    echo "----------------------------------------------------"
    
    # Run clean and test with property overrides
    if ./gradlew clean test \
        -PmcVersionOverride="$MC_VER" \
        -PneoVersionOverride="$NEO_VER" --no-daemon; then
        echo "| $MC_VER | $NEO_VER | ✅ PASS | Compiled and tested successfully |" >> $REPORT
    else
        echo "| $MC_VER | $NEO_VER | ❌ FAIL | Compilation or tests failed |" >> $REPORT
    fi
done

echo "" >> $REPORT
echo "Validation complete."
cat $REPORT
