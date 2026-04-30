package org.example.datapipeline.plugin;

/**
 * Service Provider Interface (SPI) for contributing custom action types to the pipeline.
 *
 * <p>Third-party code can introduce new action types (e.g. {@code send_email},
 * {@code generate_pdf}, {@code http_request}) without modifying the framework core by:
 * <ol>
 *   <li>Implementing this interface in a concrete class.</li>
 *   <li>Registering the implementation in a
 *       {@code META-INF/services/org.example.datapipeline.plugin.ActionPlugin} file on the
 *       classpath (one fully-qualified class name per line).</li>
 * </ol>
 *
 * <p>During framework startup, {@link org.example.datapipeline.executor.action.ActionRegistry}
 * discovers all registered {@code ActionPlugin} implementations via
 * {@link java.util.ServiceLoader}, wraps each in a
 * {@link org.example.datapipeline.plugin.PluginAdapter}, and registers the adapter under
 * the type returned by {@link #getType()}.
 *
 * <p>The plugin is then invocable from the pipeline XML as:
 * <pre>{@code
 * <action type="<getType() return value>">
 *   <method name="...">
 *     <param name="..." value="..."/>
 *   </method>
 * </action>
 * }</pre>
 */
public interface ActionPlugin {

    /**
     * Returns the action type identifier used to look up this plugin in the registry.
     *
     * <p>Must be unique across all built-in and plugin action types. Should be lowercase
     * and use underscores (e.g. {@code "generate_pdf"}, {@code "http_request"}).
     *
     * @return the action type string; never {@code null} or blank
     */
    String getType();

    /**
     * Returns a human-readable name for this plugin, used in logging and diagnostics.
     *
     * <p>Can be the same as {@link #getType()} or a more descriptive label.
     *
     * @return display name string
     */
    String getName();

    /**
     * Returns the {@link Executor} that performs the actual work of this plugin action.
     *
     * <p>The framework calls {@link Executor#execute(org.example.datapipeline.executor.context.ExecutionContext)}
     * on the returned executor for each task that uses this plugin. The executor receives the
     * full execution context (input iterator, output config, method params) and must return a
     * new {@link org.example.datapipeline.executor.iterator.DataIterator} representing the
     * transformed/produced data.
     *
     * <p>The executor may be returned as a lambda or method reference since {@link Executor}
     * is a {@link FunctionalInterface}.
     *
     * @return the executor for this plugin; called once per task invocation
     */
    Executor getExecutor();
}
