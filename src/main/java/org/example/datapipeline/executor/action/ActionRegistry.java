package org.example.datapipeline.executor.action;

import org.example.datapipeline.executor.action.transform.TransformAction;
import org.example.datapipeline.executor.action.join.JoinAction;

import java.util.HashMap;
import java.util.Map;

/**
 * Singleton registry that maps action type strings to their {@link ActionExecutor} instances.
 *
 * <p>The registry is initialised once in a static block when the class is first loaded.
 * Three built-in executors are registered unconditionally:
 * <ul>
 *   <li>{@code "bash"}      → {@link BashAction} – runs a shell script with I/O paths as arguments</li>
 *   <li>{@code "transform"} → {@link org.example.datapipeline.executor.action.transform.TransformAction}
 *       – delegates to one of the registered {@link org.example.datapipeline.executor.action.transform.TransformStrategy}
 *       implementations based on the method name</li>
 *   <li>{@code "join"}      → {@link org.example.datapipeline.executor.action.join.JoinAction}
 *       – performs hash or sort-merge joins between two datasets</li>
 * </ul>
 *
 * <p>After the built-ins, the registry discovers third-party action plugins via the Java
 * {@link java.util.ServiceLoader} SPI. Any JAR on the classpath that contains a
 * {@code META-INF/services/org.example.datapipeline.plugin.ActionPlugin} file listing
 * concrete {@link org.example.datapipeline.plugin.ActionPlugin} implementations will have
 * those plugins wrapped in a {@link org.example.datapipeline.plugin.PluginAdapter} and
 * registered automatically. This allows new action types to be contributed without modifying
 * the framework core.
 *
 * <p>All registered executors are shared singletons – they are looked up by type string
 * (lowercased) at task execution time and must therefore be <b>thread-safe</b>.
 */
public class ActionRegistry{

    private static final Map<String, ActionExecutor> registry = new HashMap<>();

    static {
        register(new BashAction());
        register(new TransformAction());
        register(new JoinAction());

        java.util.ServiceLoader<org.example.datapipeline.plugin.ActionPlugin> loader = 
            java.util.ServiceLoader.load(org.example.datapipeline.plugin.ActionPlugin.class);
        for (org.example.datapipeline.plugin.ActionPlugin plugin : loader) {
            register(new org.example.datapipeline.plugin.PluginAdapter(plugin));
        }
    }

    /**
     * Adds an executor to the registry under its own type key.
     *
     * <p>The key is taken from {@link ActionExecutor#getType()}, already lowercase by
     * convention. If an executor with the same type is already registered, it is silently
     * replaced (last registration wins), which allows plugins to override built-ins.
     *
     * @param action the executor instance to register; must not be {@code null}
     */
    private static void register(ActionExecutor action){
        registry.put(action.getType(), action);
    }

    /**
     * Looks up and returns the registered executor for the given action type.
     *
     * <p>The lookup is case-insensitive (type is lowercased before lookup).
     *
     * @param type the action type string declared in the pipeline XML
     *             (e.g. {@code "transform"}, {@code "join"}, {@code "bash"})
     * @return the registered {@link ActionExecutor} instance; never {@code null}
     * @throws RuntimeException if no executor is registered for the given type
     */
    public static ActionExecutor getAction(String type){
        ActionExecutor action = registry.get(type.toLowerCase());
        if (action == null) {
            throw new RuntimeException("Unsupported action type: " + type);
        }
        return action;
    }
}