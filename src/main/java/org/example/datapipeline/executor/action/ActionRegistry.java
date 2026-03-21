package org.example.datapipeline.executor.action;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains a registry of available action executors and provides
 * a centralized mechanism to retrieve them based on action type.
 *
 * Each ActionExecutor implementation registers itself with a unique
 * type identifier, allowing the execution engine to dynamically resolve
 * and invoke the appropriate action at runtime.
 *
 * This design enables easy extensibility — new action types can be added
 * simply by registering them without modifying existing execution logic.
 *
 * The registry ensures that only supported actions are executed and
 * throws an exception if an unknown action type is requested.
 */
public class ActionRegistry{

    private static final Map<String, ActionExecutor> registry = new HashMap<>();

    static {
        register(new BashAction());
        register(new TransformAction());
    }

    private static void register(ActionExecutor action){
        registry.put(action.getType(), action);
    }

    public static ActionExecutor getAction(String type){
        ActionExecutor action = registry.get(type.toLowerCase());
        if (action == null) {
            throw new RuntimeException("Unsupported action type: " + type);
        }
        return action;
    }
}