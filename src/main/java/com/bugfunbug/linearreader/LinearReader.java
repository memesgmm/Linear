package com.bugfunbug.linearreader;

import com.bugfunbug.linearreader.command.LinearCommand;
import com.bugfunbug.linearreader.config.ForgeLinearConfig;
import net.neoforged.bus.api.IEventBus;
import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.fml.ModContainer;
import net.neoforged.fml.common.Mod;
import net.neoforged.fml.config.ModConfig;
import net.neoforged.fml.event.config.ModConfigEvent;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.level.LevelEvent;
import net.neoforged.neoforge.event.server.ServerStartingEvent;
import net.neoforged.neoforge.event.server.ServerStoppingEvent;
import net.neoforged.neoforge.event.tick.ServerTickEvent;

@Mod(LinearRuntime.MOD_ID)
public class LinearReader {

    private final LinearRuntime runtime = LinearRuntime.install(ForgeMinecraftHooks.INSTANCE);

    public LinearReader(IEventBus modEventBus, ModContainer modContainer) {
        modContainer.registerConfig(ModConfig.Type.COMMON, ForgeLinearConfig.SPEC, "linearreader-server.toml");
        
        modEventBus.addListener(this::onConfigLoad);
        modEventBus.addListener(this::onConfigReload);

        NeoForge.EVENT_BUS.register(this);
        NeoForge.EVENT_BUS.addListener(LinearCommand::register);
    }

    private void onConfigLoad(ModConfigEvent.Loading event) {
        if (event.getConfig().getSpec() == ForgeLinearConfig.SPEC) {
            ForgeLinearConfig.pushToLinearConfig();
        }
    }

    private void onConfigReload(ModConfigEvent.Reloading event) {
        if (event.getConfig().getSpec() == ForgeLinearConfig.SPEC) {
            ForgeLinearConfig.pushToLinearConfig();
            LinearRuntime.LOGGER.info("[LinearReader] Config reloaded.");
        }
    }

    @SubscribeEvent
    public void onServerStarting(ServerStartingEvent event) {
        runtime.onServerStarting(event.getServer());
    }

    @SubscribeEvent
    public void onServerStopping(ServerStoppingEvent event) {
        runtime.onServerStopping();
    }

    @SubscribeEvent
    public void onLevelSave(LevelEvent.Save event) {
        LinearRuntime.LOGGER.info("[LinearReader] onLevelSave called");
        runtime.onLevelSave();
    }


    @SubscribeEvent
    public void onServerTick(ServerTickEvent.Post event) {
        runtime.onServerTick();
    }
}
