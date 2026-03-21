package org.example.datapipeline.executor.action;

import java.util.HashMap;
import java.util.Map;

public class ActionRegistry {

    private static final Map<String, ActionExecutor> registry = new HashMap<>();

    static {
        register(new BashAction());
        register(new TransformAction());
    }

    private static void register(ActionExecutor action) {
        registry.put(action.getType(), action);
    }

    public static ActionExecutor getAction(String type) {

        ActionExecutor action = registry.get(type.toLowerCase());

        if (action == null) {
            throw new RuntimeException("Unsupported action type: " + type);
        }

        return action;
    }
}