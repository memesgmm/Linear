<img src="https://img.shields.io/badge/NeoForge-1.21.x--26.x-orange?style=flat-square" alt="NeoForge Support" /> <img src="https://img.shields.io/badge/Java-21%20%7C%2025-blue?style=flat-square" alt="Java Support" /> <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square" alt="MIT License" /> <img src="https://img.shields.io/badge/Zstd-1.5.5--11-purple?style=flat-square" alt="Zstd" />

# Linear

**Linear** is a high-performance NeoForge mod that replaces Minecraft's standard Anvil (`.mca`) region file format with the compressed [`.linear` format](https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools), delivering dramatic reductions in world save size and save latency.

Starting with **v1.1**, Linear supports both the stable 1.21.x lifecycle and the new experimental 26.x NeoForge branches.

---

## 🛠️ Versioning & Compatibility

Linear provides two distinct builds to cover different NeoForge lifecycles:

| Build Target | Minecraft / NeoForge | Required Java | Filename Pattern |
| :--- | :--- | :--- | :--- |
| **Legacy** | 1.21.x (1.21.1 – 1.21.11) | **Java 21** | `Linear-1.21.x-1.1.0.jar` |
| **Modern** | 26.x+ (Experimental) | **Java 25** | `Linear-26.x-1.1.0.jar` |

> [!IMPORTANT]
> Make sure you download the correct JAR for your Java version. The 26.x build **requires Java 25** and will not launch on older runtimes.

---

## ⚡ Performance

Linear is designed for efficiency at scale. Below are the results from a controlled A/B test comparing a vanilla Anvil (`.mca`) world against the same world stored in `.linear`.

### 📊 Real-World Results

![Linear Benchmark Graph](https://raw.githubusercontent.com/memesgmm/Linear/main/assets/benchmark_graph.svg)

| Metric | Anvil (Vanilla) | Linear (Compressed) | Impact |
| :--- | :--- | :--- | :--- |
| **Total World Size** | **193.05 MB** | **65.69 MB** | **66% Space Saved** |
| **Region Files** | 88 files | 59 files | 33% fewer files |
| **Avg. Save Latency** | ~4,200 ms | **~25 ms** | **160x Faster Saves** |

---

### 🚀 Projected Savings at Scale
Based on the verified 66% reduction ratio, here is how Linear scales for larger servers:

| World Size (Vanilla) | World Size (Linear) | Disk Space Saved |
| :--- | :--- | :--- |
| 1 GB | **~340 MB** | 0.66 GB |
| 10 GB | **~3.4 GB** | 6.60 GB |
| 100 GB | **~34 GB** | **66.00 GB** |
| 1 TB | **~348 GB** | **676.00 GB** |

> ### 💡 TIP
> Linear's whole-region Zstd compression becomes even more effective as worlds grow, as it can find more patterns across chunk boundaries than per-chunk compression can.

---

## ⚠️ Early Development

> ### ⚠️ CAUTION
> **This mod is in early development.** While it has been tested, it modifies core world storage logic. 
> **Always keep backups of your world.** The conversion from Anvil to Linear is automatic, but reversing it requires keeping a backup of your old world files.

---

## 📖 Documentation

For detailed information on installation, configuration, commands, and technical architecture, please refer to the [WIKI.md](WIKI.md).

---

### Credits
- Based on the original [LinearReader](https://github.com/Bugfunbug/LinearReader) mod by **Bugfunbug**.
- The `.linear` file format was designed by [xymb-endcrystalme](https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools).

### License
Linear is released under the **MIT License**.
