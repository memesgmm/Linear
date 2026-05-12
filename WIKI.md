# Linear Documentation

This document contains detailed information on installation, configuration, and the technical architecture of the Linear mod.

## Table of Contents

- [Installation](#installation)
- [How It Works](#how-it-works)
- [Automatic World Conversion](#automatic-world-conversion)
- [Configuration](#configuration)
- [Compatibility](#compatibility)
- [Commands](#commands)
- [Building from Source](#building-from-source)
- [License](#license)

---

## Installation

1. Download the correct JAR for your NeoForge version from [Releases](https://github.com/memesgmm/Linear/releases):
    *   **1.21.1 – 1.21.11**: Use `Linear-1.21.x-1.1.0.jar` (**Requires Java 21**).
    *   **26.1.x+**: Use `Linear-26.x-1.1.0.jar` (**Requires Java 25**).
2. Place the JAR in your NeoForge `mods/` folder.
3. Ensure your Java runtime matches the requirement (Java 25 for NeoForge 26+).
4. Start your server or client.

On first launch, Linear will automatically convert any existing `.mca` world data to `.linear` format before the world loads. The original `.mca` files are deleted after a successful conversion.

> [!WARNING]
> **Back up your world before installing for the first time.** The conversion is irreversible without a backup.
> Linear will refuse to delete `.mca` files if the conversion fails, allowing you to retry safely.

---

## How It Works

### The `.linear` Format
The `.linear` format stores all 1024 chunks of a region in a single contiguous file, compressed as a whole with **Zstd**.

### Storage Architecture
```
RegionFileStorage
  └── RegionFileStorageMixin         ← replaces all read/write/flush/close
        ├── linearCache              ← LRU map: region coord → LinearRegionFile
        └── regionCache              ← LRU map: region coord → LinearBackedRegionFile
              └── LinearBackedRegionFile  ← RegionFile subclass (vanilla & C2ME compat)
                    └── LinearRegionFile       ← core: loading, writing, flushing
                          └── ZstdSupport      ← isolated classloader for zstd-jni
```

### Write Path
1. Raw (uncompressed) NBT bytes are stored in memory — region is marked `dirty`.
2. After a configurable quiet period, `LinearRuntime` submits the region to a background executor.
3. The executor compresses the entire region with Zstd and atomically renames a `.wip` file to the final `.linear` path.

### Read Path
1. A call to `read(ChunkPos)` reaches `RegionFileStorageMixin`.
2. `LinearRegionFile.read()` triggers `loadIfNeeded()` under a per-region write-lock on first access.
3. Subsequent reads use a per-region read-lock, allowing full concurrency across the region.

---

## Automatic World Conversion

Linear converts `.mca` files automatically when a world dimension is first opened. 
- Each `.mca` file is read with vanilla `RegionFile` and written verbatim to a new `LinearRegionFile`.
- Idempotent: if a `.linear` file already exists, the corresponding `.mca` is deleted.
- Failed conversions leave the original `.mca` intact.

---

## Configuration

Configuration is stored in `config/linear-server.toml`:

- `compressionLevel`: Zstd compression level (1–22). Default: 6.
- `syncWrites`: Whether to write in dsync mode (safer but slower).
- `backup.enabled`: Enable automatic `.bak` rotation.
- `backup.intervalMinutes`: Minutes between backup refreshes.

---

## Compatibility

| Mod | Status | Notes |
| :--- | :---: | :--- |
| **NeoForge 1.21.x** | ✅ | Stable lifecycle (Java 21) |
| **NeoForge 26.x+** | ✅ | Modern lifecycle (Java 25) |
| **C2ME** | ✅ | Full async write/clear path intercepted |
| **Distant Horizons** | ✅ | Pregen monitor pauses eviction during heavy pregen |
| **Sable / Sublevels** | ✅ | Automatic conversion for each sub-level |

Linear does **not** alter the NBT data inside chunks. Any mod that reads vanilla NBT will work without modification.

---

## Commands

Requires operator permission level 4.

- `/linear stats`: Displays live I/O statistics and memory usage.
- `/linear prune-chunks`: Dry-run analysis for safe chunk deletion.
- `/linear prune-chunks confirm`: Permanently delete empty/never-visited chunks.
- `/linear sync-backups`: Force an immediate backup refresh of all loaded regions.

---

## Building from Source

**Requirements:** JDK 21 (for Legacy) or JDK 25 (for Modern), Gradle 8.10+

```bash
git clone https://github.com/memesgmm/Linear.git
cd Linear

# Build for Legacy (1.21.x / Java 21)
./gradlew jar -PbuildTarget=legacy

# Build for Modern (26.x / Java 25)
./gradlew jar -PbuildTarget=modern

# Build both and copy to build/libs/
./scripts/build_all.sh
```

---

## License

Linear is released under the **MIT License**.
The bundled [zstd-jni](https://github.com/luben/zstd-jni) library is released under the BSD 2-Clause License.
The `.linear` file format was designed by [xymb-endcrystalme](https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools).
