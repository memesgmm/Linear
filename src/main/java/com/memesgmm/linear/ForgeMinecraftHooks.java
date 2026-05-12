package com.memesgmm.linear;

import net.minecraft.resources.ResourceKey;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.storage.LevelResource;

import java.nio.file.Path;

final class ForgeMinecraftHooks implements MinecraftHooks {

    static final ForgeMinecraftHooks INSTANCE = new ForgeMinecraftHooks();

    private ForgeMinecraftHooks() {}

    @Override
    public Path resolveWorldRoot(MinecraftServer server) {
        return server.getWorldPath(LevelResource.ROOT);
    }

    @Override
    public Path resolveRegionFolder(Path worldRoot, ResourceKey<Level> dimension) {
        if (dimension.equals(Level.OVERWORLD)) return worldRoot.resolve("region");
        if (dimension.equals(Level.NETHER)) return worldRoot.resolve("DIM-1").resolve("region");
        if (dimension.equals(Level.END)) return worldRoot.resolve("DIM1").resolve("region");

        // Use reflection to get namespace/path to handle ResourceLocation -> Identifier rename in 1.21.11
        String namespace;
        String path;
        try {
            Object id;
            try {
                // 1.21.1 - 1.21.10: ResourceKey.location() -> ResourceLocation
                id = dimension.getClass().getMethod("location").invoke(dimension);
            } catch (NoSuchMethodException e) {
                // 1.21.11+: ResourceKey.identifier() -> Identifier
                id = dimension.getClass().getMethod("identifier").invoke(dimension);
            }
            
            namespace = (String) id.getClass().getMethod("getNamespace").invoke(id);
            path = (String) id.getClass().getMethod("getPath").invoke(id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve dimension folder for " + dimension, e);
        }

        return worldRoot.resolve("dimensions")
                .resolve(namespace)
                .resolve(path)
                .resolve("region");
    }
}
