<img src="https://img.shields.io/badge/NeoForge-1.21.1-orange?style=flat-square" alt="NeoForge 1.21.1" /> <img src="https://img.shields.io/badge/Java-21-blue?style=flat-square" alt="Java 21" /> <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square" alt="MIT License" /> <img src="https://img.shields.io/badge/Zstd-1.5.5--11-purple?style=flat-square" alt="Zstd" />

# Linear

**Linear** is a high-performance NeoForge 1.21.1 mod that replaces Minecraft's standard Anvil (`.mca`) region file format with the compressed [`.linear` format](https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools), delivering dramatic reductions in world save size and save latency.

---

## ⚡ Performance

![Linear Benchmark Graph](assets/benchmark_graph.svg)

In real-world testing (Seed: `9131502383106133584`), Linear reduced the total world save size from **193 MB** to **66 MB** (**66% total saving**), including all entities and metadata.

- 🗜️ **Up to 80% smaller** region files.
- ⚡ **Asynchronous background saves** — compression happens on a background thread, eliminating server-thread stalls.
- 📖 **Comparable or faster** chunk reads under load.

---

## ⚠️ Early Development

> [!CAUTION]
> **This mod is in early development.** While it has been tested, it modifies core world storage logic. 
> **Always keep backups of your world.** The conversion from Anvil to Linear is automatic, but reversing it requires manual export.

---

## 📖 Documentation

For detailed information on installation, configuration, commands, and technical architecture, please refer to the [WIKI.md](WIKI.md).

---

### Credits
- Based on the original [LinearReader](https://github.com/Bugfunbug/LinearReader) mod by **Bugfunbug**.
- The `.linear` file format was designed by [xymb-endcrystalme](https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools).

### License
Linear is released under the **MIT License**.
