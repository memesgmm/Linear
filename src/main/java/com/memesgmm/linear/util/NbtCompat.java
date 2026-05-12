package com.memesgmm.linear.util;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.Tag;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;

/**
 * Utility for cross-version NBT compatibility between 1.21.1 and 1.21.5+.
 */
public final class NbtCompat {
    private static final Method GET_ALL_KEYS;
    private static final Method CONTAINS_WITH_TYPE;
    private static final Method GET_COMPOUND;
    private static final Method GET_LIST;
    private static final Method GET_LONG;

    static {
        Method getAllKeys = null;
        try {
            getAllKeys = CompoundTag.class.getMethod("getAllKeys");
        } catch (NoSuchMethodException e) {
            try {
                getAllKeys = CompoundTag.class.getMethod("getKeys");
            } catch (NoSuchMethodException ex) {
                try {
                    getAllKeys = CompoundTag.class.getMethod("keySet");
                } catch (NoSuchMethodException ex2) {}
            }
        }
        GET_ALL_KEYS = getAllKeys;

        Method containsWithType = null;
        try {
            containsWithType = CompoundTag.class.getMethod("contains", String.class, int.class);
        } catch (NoSuchMethodException e) {
            try {
                containsWithType = CompoundTag.class.getMethod("contains", String.class, byte.class);
            } catch (NoSuchMethodException ex) {}
        }
        CONTAINS_WITH_TYPE = containsWithType;

        Method getCompound = null;
        try {
            // Prefer 1.21.5+ getCompoundOrEmpty
            getCompound = CompoundTag.class.getMethod("getCompoundOrEmpty", String.class);
        } catch (NoSuchMethodException e) {
            try {
                // 1.21.1 getCompound(String)
                getCompound = CompoundTag.class.getMethod("getCompound", String.class);
            } catch (NoSuchMethodException ex) {}
        }
        GET_COMPOUND = getCompound;

        Method getList = null;
        try {
            // Prefer 1.21.5+ getListOrEmpty
            getList = CompoundTag.class.getMethod("getListOrEmpty", String.class);
        } catch (NoSuchMethodException e) {
            try {
                // 1.21.1 getList(String, int)
                getList = CompoundTag.class.getMethod("getList", String.class, int.class);
            } catch (NoSuchMethodException ex) {
                try {
                    // 1.21.5 raw getList(String) -> Optional
                    getList = CompoundTag.class.getMethod("getList", String.class);
                } catch (NoSuchMethodException ex2) {}
            }
        }
        GET_LIST = getList;

        Method getLong = null;
        try {
            // Prefer 1.21.5+ getLongOr
            getLong = CompoundTag.class.getMethod("getLongOr", String.class, long.class);
        } catch (NoSuchMethodException e) {
            try {
                // 1.21.1 getLong(String)
                getLong = CompoundTag.class.getMethod("getLong", String.class);
            } catch (NoSuchMethodException ex) {}
        }
        GET_LONG = getLong;
    }

    public static final int TAG_ANY_NUMERIC = 99;

    public static Set<String> getAllKeys(CompoundTag tag) {
        if (GET_ALL_KEYS != null) {
            try {
                return (Set<String>) GET_ALL_KEYS.invoke(tag);
            } catch (Exception ignored) {}
        }
        return Collections.emptySet();
    }

    public static CompoundTag getCompound(CompoundTag tag, String key) {
        if (GET_COMPOUND != null) {
            try {
                Object result = GET_COMPOUND.invoke(tag, key);
                if (result instanceof java.util.Optional opt) {
                    return opt.isPresent() ? (CompoundTag) opt.get() : new CompoundTag();
                }
                return (CompoundTag) result;
            } catch (Exception ignored) {}
        }
        return new CompoundTag();
    }

    public static long getLong(CompoundTag tag, String key) {
        if (GET_LONG != null) {
            try {
                if (GET_LONG.getParameterCount() == 2) {
                    return (long) GET_LONG.invoke(tag, key, 0L);
                }
                Object result = GET_LONG.invoke(tag, key);
                if (result instanceof java.util.Optional opt) {
                    return opt.isPresent() ? (long) opt.get() : 0L;
                }
                return (long) result;
            } catch (Exception ignored) {}
        }
        return 0L;
    }

    public static ListTag getList(CompoundTag tag, String key, int type) {
        if (GET_LIST != null) {
            try {
                Object result;
                if (GET_LIST.getParameterCount() == 2) {
                    result = GET_LIST.invoke(tag, key, GET_LIST.getParameterTypes()[1] == byte.class ? (byte) type : type);
                } else {
                    result = GET_LIST.invoke(tag, key);
                }

                if (result instanceof java.util.Optional opt) {
                    return opt.isPresent() ? (ListTag) opt.get() : new ListTag();
                }
                return (ListTag) result;
            } catch (Exception ignored) {}
        }
        return new ListTag();
    }

    public static boolean contains(CompoundTag tag, String key, int type) {
        if (CONTAINS_WITH_TYPE != null) {
            try {
                if (CONTAINS_WITH_TYPE.getParameterTypes()[1] == byte.class) {
                    return (boolean) CONTAINS_WITH_TYPE.invoke(tag, key, (byte) type);
                }
                return (boolean) CONTAINS_WITH_TYPE.invoke(tag, key, type);
            } catch (Exception ignored) {}
        }
        // Manual type check for all versions (1.21.1 - 1.21.5+)
        net.minecraft.nbt.Tag t = tag.get(key);
        if (t != null) {
            byte id = t.getId();
            if (type == TAG_ANY_NUMERIC) {
                return id >= 1 && id <= 6;
            }
            return id == (byte) type;
        }
        return false;
    }
}
