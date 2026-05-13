package com.memesgmm.linear.linear;

import com.memesgmm.linear.LinearRuntime;
import com.memesgmm.linear.LinearTestSupport;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.IntTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.world.level.ChunkPos;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class LinearStressTest {

    private Path tempDir;
    private Path regionPath;

    @BeforeEach
    void setUp() throws IOException {
        LinearTestSupport.resetState();
        tempDir = Files.createTempDirectory("linear-stress-test-");
        regionPath = tempDir.resolve("r.0.0.linear");
    }

    @AfterEach
    void tearDown() throws IOException {
        LinearTestSupport.resetState();
        LinearTestSupport.deleteTree(tempDir);
    }

    @Test
    void testHighConcurrencyWithAsyncFlushes() throws Exception {
        // This test simulates high concurrency I/O while background flushes are occurring.
        // It specifically monitors for any lock contention that would cause stalls > 500ms.

        final int NUM_WRITERS = 4;
        final int NUM_READERS = 4;
        final int TEST_DURATION_MS = 5000;
        final long MAX_ALLOWED_STALL_MS = 1000; // Even 1 second is very bad, but we use it to catch the 2-10s stall.

        LinearBackedRegionFile regionFile = LinearBackedRegionFile.create(new LinearRegionFile(regionPath, false, LinearTestSupport.dummyStorageInfo()));

        ExecutorService ioWorker = Executors.newSingleThreadExecutor();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_WRITERS + NUM_READERS + 1);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicReference<String> stallError = new AtomicReference<>();

        // Map to keep track of the latest "version" written to each chunk.
        // To avoid race conditions in the map vs the file, we assign chunks to specific writers.
        Map<ChunkPos, Integer> expectedChunkVersions = new ConcurrentHashMap<>();
        AtomicInteger totalWrites = new AtomicInteger(0);
        AtomicInteger totalReads = new AtomicInteger(0);

        for (int t = 0; t < NUM_WRITERS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    int version = 0;
                    while (running.get()) {
                        // Each thread gets a distinct set of chunks to write to (e.g. x % NUM_WRITERS == threadId)
                        int rx = (int) (Math.random() * 32);
                        int rz = (int) (Math.random() * 32);
                        if (rx % NUM_WRITERS != threadId) continue;

                        ChunkPos pos = new ChunkPos(rx, rz);
                        version++;
                        
                        CompoundTag tag = new CompoundTag();
                        tag.put("Version", IntTag.valueOf(version));
                        
                        // Pad with some junk data to make compression actually do some work
                        int[] junk = new int[1024];
                        for(int i=0; i<1024; i++) junk[i] = (int)(Math.random() * Integer.MAX_VALUE);
                        tag.put("Junk", new net.minecraft.nbt.IntArrayTag(junk));

                        long startNs = System.nanoTime();
                        try (DataOutputStream dos = regionFile.getChunkDataOutputStream(pos)) {
                            NbtIo.write(tag, dos);
                        }
                        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                        if (elapsedMs > MAX_ALLOWED_STALL_MS) {
                            stallError.set("Write operation stalled for " + elapsedMs + "ms!");
                        }
                        
                        expectedChunkVersions.put(pos, version);
                        totalWrites.incrementAndGet();
                        
                        Thread.sleep(5); // Don't entirely hog the CPU
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        for (int t = 0; t < NUM_READERS; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (running.get()) {
                        int rx = (int) (Math.random() * 32);
                        int rz = (int) (Math.random() * 32);
                        ChunkPos pos = new ChunkPos(rx, rz);

                        long startNs = System.nanoTime();
                        // Submit the read to the IOWorker queue, just like Minecraft does
                        ioWorker.submit(() -> {
                            try (DataInputStream dis = regionFile.getChunkDataInputStream(pos)) {
                                if (dis != null) {
                                    NbtIo.read(dis); // Just ensure it's readable
                                }
                            }
                            return null;
                        }).get(); // Block waiting for the IOWorker to finish (simulating a synchronous chunk load)
                        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                        if (elapsedMs > MAX_ALLOWED_STALL_MS) {
                            stallError.set("Read operation stalled for " + elapsedMs + "ms!");
                        }
                        
                        totalReads.incrementAndGet();
                        Thread.sleep(5);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Simulating the IOWorker or periodic async flushes
        executor.submit(() -> {
            try {
                startLatch.await();
                while (running.get()) {
                    long startNs = System.nanoTime();
                    // Submit the flush to the IOWorker queue, just like Minecraft does
                    ioWorker.submit(() -> {
                        try {
                            regionFile.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                    
                    if (elapsedMs > MAX_ALLOWED_STALL_MS) {
                        stallError.set("Flush operation stalled the calling thread for " + elapsedMs + "ms!");
                    }
                    Thread.sleep(200); // Flush every 200ms
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        startLatch.countDown(); // Release the hounds
        
        Thread.sleep(TEST_DURATION_MS);
        running.set(false);
        executor.shutdown();
        ioWorker.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Threads did not terminate in time");

        assertNull(stallError.get(), stallError.get());
        assertTrue(totalWrites.get() > 100, "Not enough writes occurred: " + totalWrites.get());
        assertTrue(totalReads.get() > 100, "Not enough reads occurred: " + totalReads.get());

        // Final verification
        // Give the background flush executor a moment to drain the queue
        Thread.sleep(1000); 
        
        // Force a final blocking flush to ensure disk matches memory before we reopen
        // We do this by getting the underlying LinearRegionFile via reflection
        try {
            java.lang.reflect.Field linearField = LinearBackedRegionFile.class.getDeclaredField("linear");
            linearField.setAccessible(true);
            LinearRegionFile internalLinear = (LinearRegionFile) linearField.get(regionFile);
            LinearRuntime.flushRegionsBlocking(java.util.List.of(internalLinear));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        regionFile.close();

        // Reopen from disk
        LinearBackedRegionFile recoveredFile = LinearBackedRegionFile.create(new LinearRegionFile(regionPath, false, LinearTestSupport.dummyStorageInfo()));

        int chunksVerified = 0;
        for (Map.Entry<ChunkPos, Integer> entry : expectedChunkVersions.entrySet()) {
            ChunkPos pos = entry.getKey();
            int expectedVersion = entry.getValue();

            try (DataInputStream dis = recoveredFile.getChunkDataInputStream(pos)) {
                assertNotNull(dis, "Chunk missing from disk: " + pos);
                CompoundTag tag = NbtIo.read(dis);
                assertEquals(expectedVersion, tag.getInt("Version"), "Data corruption at " + pos);
                chunksVerified++;
            }
        }
        
        System.out.println("Stress test completed successfully. Writes: " + totalWrites.get() + ", Reads: " + totalReads.get() + ", Verified: " + chunksVerified);
    }
}
