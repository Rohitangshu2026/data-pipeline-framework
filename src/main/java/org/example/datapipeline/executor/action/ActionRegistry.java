package org.example.datapipeline.executor.action;

import org.example.datapipeline.executor.action.transform.TransformAction;
import org.example.datapipeline.executor.action.join.JoinAction;

import java.util.HashMap;
import java.util.Map;

/**
 * Acts as a central registry for all available action executors in the pipeline.
 *
 * Maps action types to their corresponding implementations and provides
 * a lookup mechanism to retrieve them during execution.
 *
 * Executors are registered once and reused, allowing the pipeline to
 * dynamically resolve and execute actions based on configuration.
 *
 * Supports:
 * - bash      : executes shell scripts
 * - transform : performs in-memory data transformations
 *
 * Throws an exception if an unsupported action type is requested.
 *
 * This design enables easy extensibility by allowing new actions to be
 * added through registration without modifying existing logic.
 */
public class ActionRegistry{

    private static final Map<String, ActionExecutor> registry = new HashMap<>();

    static {
        register(new BashAction());
        register(new TransformAction());
        register(new JoinAction());
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