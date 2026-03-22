package org.example.datapipeline.executor.action;

import org.example.datapipeline.executor.context.ExecutionContext;

/**
 * Defines a contract for executing different types of pipeline actions.
 *
 * Implementations of this interface represent concrete action handlers
 * that can be plugged into the execution engine.
 *
 * Each action is executed using an ExecutionContext, which encapsulates
 * all necessary data including input, output, method configuration,
 * and any additional metadata required during execution.
 *
 * This abstraction enables a flexible and extensible design where new
 * action types can be introduced without modifying the core pipeline logic.
 */
public interface ActionExecutor {
    void execute(ExecutionContext ctx);
    String getType();
}