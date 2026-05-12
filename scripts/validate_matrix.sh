#!/bin/bash

# Linear Multi-Version Validation Matrix
# Tests compilation and unit tests against all supported 1.21.x versions.

set -e

VERSIONS=(
    "1.21.1:21.1.228"
    "1.21.11:21.11.42"
    "1.21.4:26.1.2.48-beta"
)

REPORT="matrix_report.md"
echo "# Version Validation Matrix Report" > $REPORT
echo "| Minecraft | NeoForge | Status | Result |" >> $REPORT
echo "| :--- | :--- | :--- | :--- |" >> $REPORT

for ENTRY in "${VERSIONS[@]}"; do
    MC_VER="${ENTRY%%:*}"
    NEO_VER="${ENTRY#*:}"
    
    TARGET="legacy"
    if [[ "$NEO_VER" == 26* ]]; then
        TARGET="modern"
    fi
    
    echo "----------------------------------------------------"
    echo "VALIDATING: Minecraft $MC_VER (NeoForge $NEO_VER) - Target: $TARGET"
    echo "----------------------------------------------------"
    
    # Run classes first (warm-up), then JUnit tests, then Headless Server test
    if ./gradlew test \
        -PbuildTarget="$TARGET" \
        -PmcVersionOverride="$MC_VER" \
        -PneoVersionOverride="$NEO_VER" --no-daemon && \
       (echo -e "linear verify\nstop" | ./gradlew runServer \
        -PbuildTarget="$TARGET" \
        -PmcVersionOverride="$MC_VER" \
        -PneoVersionOverride="$NEO_VER" --no-daemon); then
        
        if grep -q "Linear" run/logs/latest.log; then
            echo "| $MC_VER | $NEO_VER | ✅ PASS | Compiled, tested, and booted successfully ($TARGET) |" >> $REPORT
        else
            echo "| $MC_VER | $NEO_VER | ⚠️ WARN | Compiled and tested, but boot logs unclear ($TARGET) |" >> $REPORT
        fi
    else
        echo "| $MC_VER | $NEO_VER | ❌ FAIL | Compilation, tests, or boot failed ($TARGET) |" >> $REPORT
    fi
done

echo "" >> $REPORT
echo "Validation complete."
cat $REPORT
