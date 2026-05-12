package com.memesgmm.linear.util;

import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.storage.RegionFile;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;

public class LinearCompat {
    private static Method CHUNK_POS_AS_LONG;
    private static Class<?> REGION_STORAGE_INFO_CLASS;
    
    private static Method CHUNK_POS_X_METHOD;
    private static Method CHUNK_POS_Z_METHOD;
    private static Field CHUNK_POS_X_FIELD;
    private static Field CHUNK_POS_Z_FIELD;

    static {
        try {
            CHUNK_POS_AS_LONG = ChunkPos.class.getMethod("asLong", int.class, int.class);
        } catch (NoSuchMethodException e) {
            try {
                for (Method m : ChunkPos.class.getMethods()) {
                    if ((m.getName().equals("toLong") || m.getName().equals("asLong")) && m.getParameterCount() == 2) {
                        CHUNK_POS_AS_LONG = m;
                        break;
                    }
                }
            } catch (Exception ex) {}
        }

        try {
            CHUNK_POS_X_METHOD = ChunkPos.class.getMethod("x");
            CHUNK_POS_Z_METHOD = ChunkPos.class.getMethod("z");
        } catch (NoSuchMethodException e) {
            try {
                CHUNK_POS_X_FIELD = ChunkPos.class.getField("x");
                CHUNK_POS_Z_FIELD = ChunkPos.class.getField("z");
            } catch (NoSuchFieldException ex) {
                try {
                    CHUNK_POS_X_FIELD = ChunkPos.class.getDeclaredField("x");
                    CHUNK_POS_Z_FIELD = ChunkPos.class.getDeclaredField("z");
                    CHUNK_POS_X_FIELD.setAccessible(true);
                    CHUNK_POS_Z_FIELD.setAccessible(true);
                } catch (Exception ex2) {}
            }
        }
        
        try {
            REGION_STORAGE_INFO_CLASS = Class.forName("net.minecraft.world.level.chunk.storage.RegionStorageInfo");
        } catch (ClassNotFoundException e) {
        }
    }

    public static int getChunkX(ChunkPos pos) {
        if (CHUNK_POS_X_METHOD != null) {
            try { return (int) CHUNK_POS_X_METHOD.invoke(pos); } catch (Exception e) {}
        }
        if (CHUNK_POS_X_FIELD != null) {
            try { return (int) CHUNK_POS_X_FIELD.get(pos); } catch (Exception e) {}
        }
        return 0;
    }

    public static int getChunkZ(ChunkPos pos) {
        if (CHUNK_POS_Z_METHOD != null) {
            try { return (int) CHUNK_POS_Z_METHOD.invoke(pos); } catch (Exception e) {}
        }
        if (CHUNK_POS_Z_FIELD != null) {
            try { return (int) CHUNK_POS_Z_FIELD.get(pos); } catch (Exception e) {}
        }
        return 0;
    }

    public static long chunkPosAsLong(int x, int z) {
        if (CHUNK_POS_AS_LONG != null) {
            try {
                return (long) CHUNK_POS_AS_LONG.invoke(null, x, z);
            } catch (Exception e) {}
        }
        return ((long)x & 0xFFFFFFFFL) | (((long)z & 0xFFFFFFFFL) << 32);
    }

    public static boolean hasRegionStorageInfo() {
        return REGION_STORAGE_INFO_CLASS != null;
    }
    
    public static Object createDummyStorageInfo() {
        if (REGION_STORAGE_INFO_CLASS == null) return null;
        try {
            return REGION_STORAGE_INFO_CLASS.getConstructors()[0].newInstance("dummy", null, "dummy");
        } catch (Exception e) {
            return null;
        }
    }
    
    public static RegionFile createRegionFile(Object storageInfo, Path path, Path dir, boolean dsync) throws IOException {
        try {
            if (hasRegionStorageInfo()) {
                Constructor<RegionFile> c = RegionFile.class.getConstructor(REGION_STORAGE_INFO_CLASS, Path.class, Path.class, boolean.class);
                return c.newInstance(storageInfo, path, dir, dsync);
            } else {
                try {
                    Constructor<RegionFile> c = RegionFile.class.getConstructor(Path.class, Path.class, boolean.class);
                    return c.newInstance(path, dir, dsync);
                } catch (NoSuchMethodException e) {
                    throw e;
                }
            }
        } catch (Exception e) {
            if (e instanceof IOException) throw (IOException) e;
            throw new IOException("Failed to create RegionFile", e);
        }
    }
}
