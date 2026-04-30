package org.example.datapipeline.plugin;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Functional interface representing the core execution logic of an {@link ActionPlugin}.
 *
 * <p>Plugin authors implement this interface (typically as a lambda returned from
 * {@link ActionPlugin#getExecutor()}) to describe what happens when their custom action
 * type is invoked. The framework calls {@link #execute} for each task that uses the plugin.
 *
 * <p>The contract mirrors that of {@link org.example.datapipeline.executor.action.ActionExecutor#execute}:
 * the implementation should read parameters from the context's method, obtain the upstream
 * data via {@link ExecutionContext#getIterator()}, and return a new (potentially lazy)
 * {@link DataIterator} representing the plugin's output.
 *
 * <p>Unlike {@link org.example.datapipeline.executor.action.ActionExecutor}, the return
 * value is the new iterator (rather than setting it on the context directly). The
 * {@link org.example.datapipeline.plugin.PluginAdapter} takes care of calling
 * {@link ExecutionContext#setIterator(DataIterator)} with the returned value.
 */
@FunctionalInterface
public interface Executor {

    /**
     * Executes the plugin action using the provided execution context.
     *
     * @param context the per-task execution context carrying input iterator, output config,
     *                and method parameters; must not be {@code null}
     * @return a new {@link DataIterator} representing the output of this action;
     *         the first row must be the (possibly modified) header row
     * @throws RuntimeException if required parameters are missing or execution fails
     */
    DataIterator execute(ExecutionContext context);
}
