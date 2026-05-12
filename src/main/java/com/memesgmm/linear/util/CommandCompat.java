package com.memesgmm.linear.util;

import net.minecraft.commands.CommandSourceStack;
import java.lang.reflect.Method;

public class CommandCompat {
    private static Method HAS_PERMISSION_METHOD;
    private static Object GAMEMASTER_PERMISSION;
    private static Method PERMISSIONS_METHOD;
    private static Method HAS_PERMISSION_NEW_METHOD;

    static {
        try {
            HAS_PERMISSION_METHOD = CommandSourceStack.class.getMethod("hasPermission", int.class);
        } catch (NoSuchMethodException e) {
            // New system (1.21.11 / 26.x+)
            try {
                PERMISSIONS_METHOD = CommandSourceStack.class.getMethod("permissions");
                Class<?> permissionSetClass = Class.forName("net.minecraft.server.permissions.PermissionSet");
                Class<?> permissionClass = Class.forName("net.minecraft.server.permissions.Permission");
                HAS_PERMISSION_NEW_METHOD = permissionSetClass.getMethod("hasPermission", permissionClass);

                // Try to find COMMANDS_GAMEMASTER in Permissions class first (Modern approach)
                try {
                    Class<?> permissionsClass = Class.forName("net.minecraft.server.permissions.Permissions");
                    GAMEMASTER_PERMISSION = permissionsClass.getField("COMMANDS_GAMEMASTER").get(null);
                } catch (Exception ex) {
                    // Fallback to HasCommandLevel (1.21.11 approach)
                    Class<?> hasCommandLevelClass = Class.forName("net.minecraft.server.permissions.Permission$HasCommandLevel");
                    Class<?> permissionLevelEnum = Class.forName("net.minecraft.server.permissions.PermissionLevel");
                    
                    Object gamemasterLevel = null;
                    for (Object level : permissionLevelEnum.getEnumConstants()) {
                        if (level.toString().equalsIgnoreCase("GAMEMASTERS")) {
                            gamemasterLevel = level;
                            break;
                        }
                    }

                    try {
                        Method forLevelMethod = hasCommandLevelClass.getMethod("forLevel", permissionLevelEnum);
                        GAMEMASTER_PERMISSION = forLevelMethod.invoke(null, gamemasterLevel);
                    } catch (NoSuchMethodException ex2) {
                        // Try constructor (Record in 26.x if field was missing)
                        GAMEMASTER_PERMISSION = hasCommandLevelClass.getConstructor(permissionLevelEnum).newInstance(gamemasterLevel);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to initialize CommandCompat for new permission system", ex);
            }
        }
    }

    public static boolean hasPermission(CommandSourceStack source, int level) {
        try {
            if (HAS_PERMISSION_METHOD != null) {
                return (boolean) HAS_PERMISSION_METHOD.invoke(source, level);
            } else {
                Object permissions = PERMISSIONS_METHOD.invoke(source);
                return (boolean) HAS_PERMISSION_NEW_METHOD.invoke(permissions, GAMEMASTER_PERMISSION);
            }
        } catch (Exception e) {
            return false;
        }
    }
}
