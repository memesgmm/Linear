package com.memesgmm.linear.linear;

import com.memesgmm.linear.LinearTestSupport;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.storage.RegionFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LinearReversalTest {

    @TempDir
    Path tempDir;

    @Test
    void testReversalInPlace() throws IOException {
        Path regionFolder = tempDir.resolve("region");
        Files.createDirectories(regionFolder);
        Path linearPath = regionFolder.resolve("r.0.0.linear");
        Path mcaPath = regionFolder.resolve("r.0.0.mca");

        // 1. Create a .linear file with some data
        LinearRegionFile linear = new LinearRegionFile(linearPath, false, LinearTestSupport.dummyStorageInfo());
        ChunkPos pos = new ChunkPos(0, 0);
        CompoundTag originalNbt = LinearTestData.simpleChunk("reversal-test", 0, 0);
        try {
            try (DataOutputStream dos = linear.write(pos)) {
                NbtIo.write(originalNbt, dos);
            }
            linear.flush(true);
        } finally {
            LinearRegionFile.ALL_OPEN.remove(linear);
            linear.releaseChunkData();
        }

        assertTrue(Files.exists(linearPath));
        assertFalse(Files.exists(mcaPath));

        // 2. Run reversal
        MCAConverter.revertWorld(tempDir);

        // 3. Verify results
        assertFalse(Files.exists(linearPath));
        assertTrue(Files.exists(mcaPath));

        // 4. Read back from MCA to verify data integrity
        try (RegionFile mca = com.memesgmm.linear.util.LinearCompat.createRegionFile(com.memesgmm.linear.util.LinearCompat.createDummyStorageInfo(), mcaPath, regionFolder, false)) {
            DataInputStream dis = mca.getChunkDataInputStream(pos);
            assertNotNull(dis);
            CompoundTag revertedNbt = NbtIo.read(dis);
            assertEquals(originalNbt, revertedNbt);
        }
    }
}
