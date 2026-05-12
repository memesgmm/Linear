package com.bugfunbug.linearreader.benchmark;

import com.bugfunbug.linearreader.LinearRuntime;
import com.bugfunbug.linearreader.LinearStats;
import net.minecraft.server.MinecraftServer;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Optional one-file benchmark exporter.
 *
 * <h3>How to enable</h3>
 * Set {@code pregенExport = true} in {@code linearreader-server.toml}.
 * The file is written to {@code <world>/linear-pregen-stats-<timestamp>.json}
 * on server shutdown (or via {@code /linearreader export-stats}).
 *
 * <h3>How to disable permanently</h3>
 * Either set the config flag back to false, or delete this file —
 * no other file depends on it. Remove the one {@link #onServerStopping}
 * call in {@link com.bugfunbug.linearreader.LinearReader} if you also
 * delete this class.
 *
 * <h3>What is collected</h3>
 * All data comes from {@link LinearStats#INSTANCE} which is always-on.
 * Nothing extra is instrumented; this class just serialises it to JSON.
 */
public final class PregenExporter {

    private PregenExporter() {}

    // -------------------------------------------------------------------------
    // Public entry points
    // -------------------------------------------------------------------------

    /**
     * Called from {@code LinearReader.onServerStopping()} when the feature
     * flag is enabled.  Safe to call from any thread.
     *
     * @param server the stopping server (used to locate the world folder)
     */
    public static void onServerStopping(MinecraftServer server) {
        export(server, "shutdown");
    }

    /**
     * Called from {@code /linearreader export-stats} command.
     */
    public static void onCommand(MinecraftServer server) {
        export(server, "command");
    }

    // -------------------------------------------------------------------------
    // Core export logic
    // -------------------------------------------------------------------------

    private static void export(MinecraftServer server, String trigger) {
        try {
            Path worldFolder = LinearRuntime.resolveWorldRoot(server);
            if (worldFolder == null) {
                LinearRuntime.LOGGER.warn("[LinearReader] PregенExporter: could not resolve world folder, skipping export.");
                return;
            }

            String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
                    .withZone(ZoneOffset.UTC)
                    .format(Instant.now());
            Path out = worldFolder.resolve("linear-pregen-stats-" + timestamp + ".json");

            String json = buildJson(trigger, worldFolder, timestamp);
            Files.writeString(out, json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            LinearRuntime.LOGGER.info("[LinearReader] Benchmark stats exported → {}", out);
        } catch (Exception e) {
            LinearRuntime.LOGGER.error("[LinearReader] Failed to export benchmark stats: {}", e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------------
    // JSON serialisation (no external library needed — output is simple)
    // -------------------------------------------------------------------------

    private static String buildJson(String trigger, Path worldFolder, String timestamp) throws IOException {
        LinearStats s = LinearStats.INSTANCE;

        long writes        = s.chunkWrites.sum();
        long writeNsTotal  = s.chunkWriteNs.sum();
        long writeMin      = s.minChunkWriteNs.get();
        long writeMax      = s.maxChunkWriteNs.get();

        long reads         = s.chunkReads.sum();
        long readNsTotal   = s.chunkReadNs.sum();
        long readMin       = s.minChunkReadNs.get();
        long readMax       = s.maxChunkReadNs.get();

        long flushes       = s.regionFlushes.sum();
        long flushNsTotal  = s.regionFlushNs.sum();
        long flushCompNs   = s.regionFlushCompressNs.sum();
        long flushWriteNs  = s.regionFlushWriteNs.sum();

        long uncompressed  = s.bytesUncompressed.sum();
        long compressed    = s.bytesCompressed.sum();

        long cacheHits     = s.cacheHits.sum();
        long cacheMisses   = s.cacheMisses.sum();
        long cacheTotal    = cacheHits + cacheMisses;

        long regionLoads   = s.regionLoads.sum();
        long regionLoadNs  = s.regionLoadNs.sum();

        double uptimeSec   = LinearStats.uptimeSeconds();

        // Disk scan — count .linear files and their total size
        long[] diskStats = scanLinearFiles(worldFolder);
        long linearFileCount  = diskStats[0];
        long linearTotalBytes = diskStats[1];

        double compressionPct = LinearStats.compressionPct(uncompressed, compressed);

        return String.format(Locale.ROOT, """
{
  "format": "linear-pregen-stats",
  "version": "1.0",
  "trigger": "%s",
  "timestamp": "%s",
  "uptime_seconds": %.1f,

  "chunks": {
    "written": %d,
    "read": %d,
    "avg_write_ms": %.4f,
    "avg_read_ms": %.4f,
    "min_write_ms": %.4f,
    "max_write_ms": %.4f,
    "min_read_ms": %.4f,
    "max_read_ms": %.4f
  },

  "regions": {
    "flushes": %d,
    "loads": %d,
    "avg_flush_ms": %.2f,
    "avg_load_ms": %.2f,
    "flush_compress_ms_total": %.2f,
    "flush_write_ms_total": %.2f
  },

  "compression": {
    "uncompressed_bytes": %d,
    "compressed_bytes": %d,
    "ratio_pct_saved": %.2f
  },

  "cache": {
    "hits": %d,
    "misses": %d,
    "hit_rate": %.4f
  },

  "disk": {
    "linear_file_count": %d,
    "linear_total_bytes": %d,
    "linear_total_mb": %.2f
  }
}
""",
                trigger,
                timestamp,
                uptimeSec,

                writes,
                reads,
                LinearStats.avgMs(writeNsTotal, writes),
                LinearStats.avgMs(readNsTotal, reads),
                guardMs(writeMin), guardMs(writeMax),
                guardMs(readMin),  guardMs(readMax),

                flushes,
                regionLoads,
                LinearStats.avgMs(flushNsTotal, flushes),
                LinearStats.avgMs(regionLoadNs, regionLoads),
                flushCompNs  / 1_000_000.0,
                flushWriteNs / 1_000_000.0,

                uncompressed,
                compressed,
                compressionPct,

                cacheHits,
                cacheMisses,
                cacheTotal == 0 ? 0.0 : (double) cacheHits / cacheTotal,

                linearFileCount,
                linearTotalBytes,
                linearTotalBytes / (1024.0 * 1024.0)
        );
    }

    /**
     * Walk the world folder and count all .linear files + their combined size.
     * Returns long[]{count, totalBytes}.
     */
    private static long[] scanLinearFiles(Path worldFolder) {
        long[] result = {0L, 0L};
        try (var stream = Files.walk(worldFolder)) {
            stream.filter(Files::isRegularFile)
                  .filter(p -> p.getFileName().toString().endsWith(".linear"))
                  .forEach(p -> {
                      result[0]++;
                      try { result[1] += Files.size(p); } catch (IOException ignored) {}
                  });
        } catch (IOException e) {
            LinearRuntime.LOGGER.warn("[LinearReader] PregенExporter: disk scan failed: {}", e.getMessage());
        }
        return result;
    }

    /** Converts a raw nanosecond value (possibly Long.MAX_VALUE sentinel) to ms. */
    private static double guardMs(long ns) {
        return (ns == Long.MAX_VALUE || ns == 0) ? 0.0 : ns / 1_000_000.0;
    }
}
