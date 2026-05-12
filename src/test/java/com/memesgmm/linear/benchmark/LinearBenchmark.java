package com.memesgmm.linear.benchmark;

import com.memesgmm.linear.linear.LinearRegionFile;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.storage.RegionFile;
import net.minecraft.world.level.chunk.storage.RegionStorageInfo;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.resources.ResourceKey;
import net.minecraft.core.registries.Registries;

import net.minecraft.nbt.NbtAccounter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LinearBenchmark {

    public static void main(String[] args) throws Exception {
        Path regionDir = Path.of("run/world/region");
        if (!Files.exists(regionDir)) {
            System.err.println("Region directory not found: " + regionDir);
            return;
        }

        List<Path> linearFiles = Files.list(regionDir)
                .filter(p -> p.toString().endsWith(".linear"))
                .toList();

        if (linearFiles.isEmpty()) {
            System.err.println("No .linear files found in " + regionDir);
            return;
        }

        System.out.println("Starting A/B Benchmark...");
        System.out.println("Found " + linearFiles.size() + " region files.");
        System.out.println("---------------------------------------------------------");
        System.out.printf("%-15s | %-10s | %-10s | %-10s | %-10s%n", "File", "Format", "Size (KB)", "Load (ms)", "Save (ms)");
        System.out.println("---------------------------------------------------------");

        RegionStorageInfo dummyInfo = new RegionStorageInfo("benchmark", 
            ResourceKey.create(Registries.DIMENSION, ResourceLocation.parse("minecraft:overworld")), 
            "region");

        for (Path linearPath : linearFiles) {
            runBenchmark(linearPath, dummyInfo);
        }
    }

    private static void runBenchmark(Path linearPath, RegionStorageInfo info) throws IOException {
        String baseName = linearPath.getFileName().toString().replace(".linear", "");
        Path mcaPath = linearPath.resolveSibling(baseName + ".mca.bench");

        // 1. Load Linear data
        LinearRegionFile linear = new LinearRegionFile(linearPath, false, new net.minecraft.world.level.chunk.storage.RegionStorageInfo("dummy", net.minecraft.world.level.Level.OVERWORLD, "dummy"));
        List<CompoundTag> chunks = new ArrayList<>();
        List<ChunkPos> positions = new ArrayList<>();

        long startLoadLinear = System.nanoTime();
        for (int i = 0; i < 1024; i++) {
            ChunkPos pos = new ChunkPos(i % 32, i / 32);
            if (linear.hasChunk(pos)) {
                try (DataInputStream dis = linear.read(pos)) {
                    if (dis != null) {
                        chunks.add(NbtIo.read(dis, NbtAccounter.unlimitedHeap()));
                        positions.add(pos);
                    }
                }
            }
        }
        long endLoadLinear = System.nanoTime();

        // 2. Save MCA data (to create the comparison file)
        if (Files.exists(mcaPath)) Files.delete(mcaPath);
        long startSaveMca = System.nanoTime();
        try (RegionFile mca = new RegionFile(info, mcaPath, mcaPath.getParent(), true)) {
            for (int i = 0; i < chunks.size(); i++) {
                try (DataOutputStream dos = mca.getChunkDataOutputStream(positions.get(i))) {
                    NbtIo.write(chunks.get(i), dos);
                }
            }
        }
        long endSaveMca = System.nanoTime();

        // 3. Save Linear data (for write performance measurement)
        Path linearWritePath = linearPath.resolveSibling(baseName + ".linear.bench");
        if (Files.exists(linearWritePath)) Files.delete(linearWritePath);
        long startSaveLinear = System.nanoTime();
        try (LinearRegionFile linearWrite = new LinearRegionFile(linearWritePath, false, new net.minecraft.world.level.chunk.storage.RegionStorageInfo("dummy", net.minecraft.world.level.Level.OVERWORLD, "dummy"))) {
            for (int i = 0; i < chunks.size(); i++) {
                try (DataOutputStream dos = linearWrite.write(positions.get(i))) {
                    NbtIo.write(chunks.get(i), dos);
                }
            }
            linearWrite.flush(false);
        }
        long endSaveLinear = System.nanoTime();

        // 4. Load MCA data (for read performance measurement)
        long startLoadMca = System.nanoTime();
        try (RegionFile mcaRead = new RegionFile(info, mcaPath, mcaPath.getParent(), true)) {
            for (int i = 0; i < positions.size(); i++) {
                try (DataInputStream dis = mcaRead.getChunkDataInputStream(positions.get(i))) {
                    if (dis != null) {
                        NbtIo.read(dis, NbtAccounter.unlimitedHeap());
                    }
                }
            }
        }
        long endLoadMca = System.nanoTime();

        long linearSize = Files.size(linearPath);
        long mcaSize = Files.size(mcaPath);

        System.out.printf("%-15s | %-10s | %-10.1f | %-10.2f | %-10.2f%n", 
            baseName, "MCA", mcaSize / 1024.0, (endLoadMca - startLoadMca) / 1_000_000.0, (endSaveMca - startSaveMca) / 1_000_000.0);
        System.out.printf("%-15s | %-10s | %-10.1f | %-10.2f | %-10.2f%n", 
            "", "Linear", linearSize / 1024.0, (endLoadLinear - startLoadLinear) / 1_000_000.0, (endSaveLinear - startSaveLinear) / 1_000_000.0);
        System.out.println("---------------------------------------------------------");

        // Cleanup
        Files.deleteIfExists(mcaPath);
        Files.deleteIfExists(linearWritePath);
        LinearRegionFile.ALL_OPEN.remove(linear);
    }
}
