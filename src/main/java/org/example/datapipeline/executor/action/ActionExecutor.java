package org.example.datapipeline.executor.action;

import org.example.datapipeline.executor.context.ExecutionContext;

/**
 * Strategy interface for all executable action types in the pipeline.
 *
 * <p>The pipeline supports three built-in action types – {@code transform},
 * {@code join}, and {@code bash} – each backed by a distinct implementation of
 * this interface. Third-party code can contribute additional types through the
 * {@link org.example.datapipeline.plugin.ActionPlugin} SPI, which is wrapped in a
 * {@link org.example.datapipeline.plugin.PluginAdapter} that itself implements
 * {@code ActionExecutor}.
 *
 * <p>All registered implementations are stored in
 * {@link org.example.datapipeline.executor.action.ActionRegistry} and looked up by
 * the lowercase string returned from {@link #getType()} at runtime. New action types
 * can be added without touching existing code – simply implement this interface (or
 * {@code ActionPlugin}) and register the instance.
 *
 * <h2>Contract</h2>
 * <ol>
 *   <li>Receive the shared {@link ExecutionContext} which contains the input iterator,
 *       output descriptor, and method parameters.</li>
 *   <li>Transform or produce a new {@link org.example.datapipeline.executor.iterator.DataIterator}
 *       and store it back on the context via
 *       {@link ExecutionContext#setIterator(org.example.datapipeline.executor.iterator.DataIterator)}.</li>
 *   <li>The executor framework then drains that iterator into the declared output file
 *       <em>unless</em> {@link #handlesOwnOutput()} returns {@code true}.</li>
 * </ol>
 *
 * <p>Implementations must be <b>thread-safe at the class level</b> (they are shared singletons
 * in the registry) but the {@link ExecutionContext} they receive is not shared – each task
 * gets its own context.
 */
public interface ActionExecutor {

    /**
     * Executes this action using the given execution context.
     *
     * <p>Implementations should:
     * <ol>
     *   <li>Read parameters from {@link ExecutionContext#getMethod()}.</li>
     *   <li>Obtain the upstream data via {@link ExecutionContext#getIterator()}.</li>
     *   <li>Produce a (potentially lazy) transformed iterator and set it back via
     *       {@link ExecutionContext#setIterator}.</li>
     * </ol>
     *
     * @param ctx the execution context carrying input, output, method config, and metadata;
     *            never {@code null}
     * @throws RuntimeException if required parameters are missing, columns are not found,
     *                          or the underlying operation fails
     */
    void execute(ExecutionContext ctx);

    /**
     * Returns the action type identifier used to look up this executor in the registry.
     *
     * <p>The value must be a stable, lowercase string that matches the {@code type} attribute
     * on the {@code <action>} element in the pipeline XML (e.g., {@code "transform"},
     * {@code "join"}, {@code "bash"}).
     *
     * @return lowercase action type string; never {@code null} or blank
     */
    String getType();

    /**
     * Signals whether this action writes its output file directly, bypassing the
     * framework's standard {@code writeData} step.
     *
     * <p>Most actions operate by updating the iterator on the context; the framework
     * then drains that iterator into the declared output file. When an action writes
     * the output file itself (e.g. {@link BashAction} invokes a shell script that writes
     * directly to the output path), returning {@code true} from this method prevents the
     * framework from overwriting that file with the raw input iterator.
     *
     * <p>The default implementation returns {@code false}, meaning the framework will
     * drain the context iterator into the output file after every {@link #execute} call.
     *
     * @return {@code true} if the action manages its own output file; {@code false} (default)
     *         to let the framework write the context iterator to the output file
     */
    default boolean handlesOwnOutput() {
        return false;
    }
}