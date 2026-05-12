package com.memesgmm.linear.linear;

import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.storage.RegionFile;
import sun.misc.Unsafe;

import com.memesgmm.linear.LinearStats;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * A {@link RegionFile} subclass backed by {@link LinearRegionFile}.
 *
 * <h3>Two write paths</h3>
 * <ol>
 *   <li><b>Vanilla</b> — calls {@link #getChunkDataOutputStream} (public virtual method).
 *       Caller writes raw NBT; we store it directly in the LinearRegionFile.</li>
 *   <li><b>c2me</b> — calls the private {@code RegionFile.write(ChunkPos, ByteBuffer)}
 *       via its {@code IRegionFile.invokeWriteChunk} accessor mixin, bypassing
 *       {@code getChunkDataOutputStream} entirely.  {@code RegionFileMixin} intercepts
 *       that call and delegates to {@link #writeFromBuffer}.</li>
 * </ol>
 * In both cases the LinearRegionFile stores raw (decompressed) NBT bytes, so
 * {@link #getChunkDataInputStream} works identically regardless of write path.
 *
 * <h3>Why Unsafe</h3>
 * {@code RegionFile}'s constructor opens / creates the {@code .mca} file.
 * {@link Unsafe#allocateInstance} skips all constructors, so no {@code .mca}
 * file is ever created.  Virtual dispatch is unaffected.
 *
 * <h3>Lifecycle</h3>
 * The underlying {@link LinearRegionFile} is owned by
 * {@code RegionFileStorageMixin}'s {@code linearCache}. {@link #close()} is a
 * deliberate no-op to prevent double-close when this wrapper is evicted from
 * the vanilla {@code regionCache}.
 */
public final class LinearBackedRegionFile extends RegionFile {

    // -------------------------------------------------------------------------
    // Unsafe bootstrap (one-time)
    // -------------------------------------------------------------------------

    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // -------------------------------------------------------------------------
    // Instance state
    // -------------------------------------------------------------------------

    /** Set by {@link #create} immediately after {@code allocateInstance}. */
    private LinearRegionFile linear;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Never called — present only to satisfy the compiler's super-constructor rule.
     */
    @SuppressWarnings("DataFlowIssue")
    private LinearBackedRegionFile() throws IOException {
        super(null, null, null, false); // unreachable
    }

    /**
     * Creates a {@code LinearBackedRegionFile} without touching disk.
     */
    public static LinearBackedRegionFile create(LinearRegionFile linear) throws IOException {
        try {
            LinearBackedRegionFile inst =
                    (LinearBackedRegionFile) UNSAFE.allocateInstance(LinearBackedRegionFile.class);
            inst.linear = linear;

            // Initialize critical RegionFile fields that skip constructor via Unsafe.
            // This prevents NPEs in mods like C2ME that expect these to be present.
            setInternalField(inst, "info", linear.getStorageInfo());
            setInternalField(inst, "path", linear.getPath());
            setInternalField(inst, "version", net.minecraft.world.level.chunk.storage.RegionFileVersion.VERSION_DEFLATE);

            return inst;
        } catch (InstantiationException e) {
            throw new IOException("[Linear] Cannot allocate LinearBackedRegionFile", e);
        }
    }

    private static void setInternalField(Object obj, String name, Object value) {
        try {
            Field f = net.minecraft.world.level.chunk.storage.RegionFile.class.getDeclaredField(name);
            f.setAccessible(true);
            f.set(obj, value);
        } catch (Exception e) {
            // Ignore if field doesn't exist (version differences)
        }
    }

    // -------------------------------------------------------------------------
    // Virtual method overrides (vanilla read/write path)
    // -------------------------------------------------------------------------

    /** Vanilla read path — returns raw NBT bytes stored by LinearRegionFile. */
    @Override
    public synchronized DataInputStream getChunkDataInputStream(ChunkPos pos) throws IOException {
        return linear.read(pos);
    }

    /**
     * Vanilla write path — caller writes raw NBT bytes; LinearRegionFile stores them.
     * c2me bypasses this and calls the private {@code write(ChunkPos, ByteBuffer)}
     * directly; see {@link #writeFromBuffer}.
     */
    @Override
    public DataOutputStream getChunkDataOutputStream(ChunkPos pos) throws IOException {
        return linear.write(pos);
    }

    @Override
    public boolean hasChunk(ChunkPos pos) {
        return linear.hasChunk(pos);
    }

    /**
     * No-op — the underlying {@link LinearRegionFile} is closed via
     * {@code RegionFileStorageMixin.linearCache}.
     */
    @Override
    public synchronized void close() {
        // intentionally empty — see class javadoc
    }

    /**
     * Delegates to {@link LinearRegionFile#clearChunk}.
     * Called by {@code RegionFileMixin.interceptLinearClear} when c2me invokes
     * the direct chunk-delete path on this instance.
     */
    public void clearChunk(ChunkPos pos) {
        linear.clearChunk(pos);
    }

    // -------------------------------------------------------------------------
    // c2me write path
    // -------------------------------------------------------------------------

    /**
     * Called by {@code RegionFileMixin} when c2me invokes
     * {@code RegionFile.write(ChunkPos, ByteBuffer)} on this instance.
     *
     * <p>MC chunk stream layout (as written by {@code ChunkBuffer.close()}):
     * <pre>
     *   bytes  0–3 : big-endian int   — (compressedLen + 1)
     *   byte   4   : compression type — 1=gzip, 2=deflate/zlib, 3=none
     *   bytes  5…N : compressed NBT data
     * </pre>
     * We strip the header, decompress, and store raw NBT bytes — exactly the same
     * format stored by the vanilla write path.
     */
    public void writeFromBuffer(ChunkPos pos, ByteBuffer buf) throws IOException {
        IdleRecompressor.notifyIO();

        // C2ME can hand us a windowed buffer; decode from the caller-visible range,
        // not from absolute index 0 of the underlying storage.
        ByteBuffer chunkBuf = buf.duplicate();
        if (chunkBuf.remaining() < 5) {
            throw new IOException("[Linear] Chunk buffer too short for " + pos
                    + ": " + chunkBuf.remaining() + " byte(s)");
        }

        int header = chunkBuf.getInt(); // compressedLen + 1
        if (header <= 0) {
            throw new IOException("[Linear] Invalid chunk header length for " + pos + ": " + header);
        }

        int compressionType = chunkBuf.get() & 0xFF;
        int compressedLen = header - 1;
        if (compressedLen > chunkBuf.remaining()) {
            throw new IOException("[Linear] Chunk buffer truncated for " + pos
                    + ": expected " + compressedLen + " compressed byte(s), found "
                    + chunkBuf.remaining());
        }

        byte[] compressed = new byte[compressedLen];
        chunkBuf.get(compressed);

        long t = System.nanoTime();
        try (DataOutputStream dos = linear.write(pos)) {
            writeDecompressed(compressionType, compressed, dos);
        }
        LinearStats.recordChunkWrite(System.nanoTime() - t);
    }

    /**
     * Streams {@code data} according to the MC chunk stream compression type.
     */
    private static void writeDecompressed(int type, byte[] data, DataOutputStream out) throws IOException {
        switch (type) {
            case 1: { // GZip
                try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(data))) {
                    in.transferTo(out);
                    return;
                }
            }
            case 2: { // Zlib / Deflate
                try (InputStream in = new InflaterInputStream(new ByteArrayInputStream(data))) {
                    in.transferTo(out);
                    return;
                }
            }
            case 3:   // None (uncompressed)
                out.write(data);
                return;
            default:
                throw new IOException(
                        "[Linear] Unsupported MC compression type in c2me write path: " + type
                                + " — please report this.");
        }
    }
}
