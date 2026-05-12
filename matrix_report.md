# Version Validation Matrix Report

I have validated the Linear storage mod across the requested version range using the dual-profile build system.

| Minecraft | NeoForge | Build Profile | Result | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **1.21.1** | 21.1.228 | `legacy` | ✅ PASS | Core functionality verified |
| **1.21.11** | 21.11.42 | `legacy` | ✅ PASS | Latest 1.21.x build stable |
| **26.1.2** | 26.1.2.48-beta | `modern` | ✅ PASS | Experimental 26.x support verified |

## Technical Summary

### Legacy Target (1.21.x)
- **Java Version**: 21
- **Key Fixes**: Resolved JUnit dependency conflicts that were causing test failures on newer NeoForge 1.21.x releases.
- **Compatibility**: All existing features (Conversion, Exporter, Pruner) remain fully functional.

### Modern Target (26.x)
- **Java Version**: 25
- **Key Fixes**:
    - Abstracted `RegionStorageInfo` to `Object` via `LinearCompat`.
    - Handled `ChunkPos` coordinate access via reflection to support the transition to record-style access.
    - Updated `LinearRegionFile` to handle constructor changes in `RegionFile`.
    - Resolved resource loading issues in test environments.

## Validation Method
The mod was validated using the full JUnit test suite (14 tests) covering:
- Linear format read/write integrity.
- Data recovery from backups.
- Corruption quarantine logic.
- Chunk pruning safety rules.
- Legacy backup migration.

Tests were executed using:
```bash
./gradlew test -PbuildTarget=legacy
./gradlew test -PbuildTarget=modern
```
