package com.memesgmm.linear.command;

import net.neoforged.neoforge.event.RegisterCommandsEvent;

@net.neoforged.fml.common.EventBusSubscriber(modid = com.memesgmm.linear.LinearRuntime.MOD_ID)
public final class LinearCommand {
    
    private LinearCommand() {}

    @net.neoforged.bus.api.SubscribeEvent
    public static void register(RegisterCommandsEvent event) {
        LinearCommandRegistrar.register(event.getDispatcher());
    }
}
