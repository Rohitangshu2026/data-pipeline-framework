package org.example.datapipeline.plugin;

import org.example.datapipeline.executor.action.ActionExecutor;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Adapter that bridges an {@link ActionPlugin} into the {@link ActionExecutor} interface.
 *
 * <p>The pipeline's core execution engine works exclusively with {@link ActionExecutor}
 * instances. Third-party plugins implement the lighter-weight {@link ActionPlugin} SPI.
 * {@code PluginAdapter} wraps an {@link ActionPlugin} so it can be registered in
 * {@link org.example.datapipeline.executor.action.ActionRegistry} and invoked like any
 * built-in action.
 *
 * <p>The adapter:
 * <ol>
 *   <li>Delegates {@link #getType()} to {@link ActionPlugin#getType()} (lowercased).</li>
 *   <li>In {@link #execute(ExecutionContext)}, calls
 *       {@link ActionPlugin#getExecutor()}{@code .execute(ctx)} to get the plugin's output
 *       iterator and stores it back on the context via
 *       {@link ExecutionContext#setIterator(DataIterator)}.</li>
 * </ol>
 *
 * <p>The adapter does <em>not</em> override {@link #handlesOwnOutput()}, so it defaults to
 * {@code false} – the framework will write the returned iterator to the output file normally.
 * Plugins that write their own output must implement a thin {@code ActionExecutor} directly
 * rather than using the plugin SPI.
 */
public class PluginAdapter implements ActionExecutor {

    private final ActionPlugin plugin;

    /**
     * Creates an adapter for the given plugin.
     *
     * @param plugin the plugin implementation to wrap; must not be {@code null}
     */
    public PluginAdapter(ActionPlugin plugin) {
        this.plugin = plugin;
    }

    /**
     * Invokes the plugin's executor, then stores the resulting iterator on the context.
     *
     * @param ctx the per-task execution context
     */
    @Override
    public void execute(ExecutionContext ctx) {
        DataIterator newIterator = plugin.getExecutor().execute(ctx);
        ctx.setIterator(newIterator);
    }

    /**
     * Returns the plugin's type string (lowercased) so it can be matched against the
     * {@code type} attribute in the pipeline XML.
     *
     * @return lowercase type string from {@link ActionPlugin#getType()}
     */
    @Override
    public String getType() {
        return plugin.getType().toLowerCase();
    }
}
