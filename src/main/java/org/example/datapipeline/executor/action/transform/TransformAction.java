package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Action executor for all row-level transformation operations.
 *
 * <p>{@code TransformAction} is the context in the <em>Strategy</em> pattern. It maintains an
 * internal registry of {@link TransformStrategy} implementations keyed by method name, and
 * dispatches each incoming task to the correct strategy based on the method declared in the
 * pipeline XML.
 *
 * <p>Registered strategies and their method names:
 * <table border="1" summary="registered transform strategies">
 *   <tr><th>Method name</th><th>Strategy class</th><th>Description</th></tr>
 *   <tr><td>{@code filter}</td><td>{@link FilterStrategy}</td><td>Retain rows matching a predicate</td></tr>
 *   <tr><td>{@code select}</td><td>{@link SelectStrategy}</td><td>Project a subset of columns</td></tr>
 *   <tr><td>{@code map}</td><td>{@link MapStrategy}</td><td>Apply arithmetic to one column</td></tr>
 *   <tr><td>{@code aggregate}</td><td>{@link AggregateStrategy}</td><td>GROUP BY + SUM/AVG/MIN/MAX/COUNT</td></tr>
 *   <tr><td>{@code derive}</td><td>{@link DeriveStrategy}</td><td>Compute a new column from a formula</td></tr>
 *   <tr><td>{@code drop_nulls}</td><td>{@link DropNullsStrategy}</td><td>Remove rows with empty/null columns</td></tr>
 *   <tr><td>{@code fill_nulls}</td><td>{@link FillNullsStrategy}</td><td>Replace empty/null with a default</td></tr>
 *   <tr><td>{@code sort}</td><td>{@link SortStrategy}</td><td>Sort all rows by a column (buffers in memory)</td></tr>
 *   <tr><td>{@code limit}</td><td>{@link LimitStrategy}</td><td>Emit at most N rows</td></tr>
 *   <tr><td>{@code normalize}</td><td>{@link NormalizeStrategy}</td><td>Min-max normalise a column to [0, 1]</td></tr>
 *   <tr><td>{@code scale}</td><td>{@link ScaleStrategy}</td><td>Z-score standardise a column (mean=0, σ=1)</td></tr>
 *   <tr><td>{@code max}</td><td>{@link MaxStrategy}</td><td>Compute the global maximum of a column</td></tr>
 * </table>
 *
 * <p>The execution flow:
 * <ol>
 *   <li>Look up the strategy by {@code method.getName()} (lowercased).</li>
 *   <li>Call {@link TransformStrategy#apply(DataIterator, Method)}, passing the current
 *       input iterator and the method configuration.</li>
 *   <li>Replace the context's iterator with the returned output iterator.</li>
 * </ol>
 *
 * <p>This design keeps all transform logic in discrete, testable strategy classes while
 * the action class itself remains a thin dispatcher.
 */
public class TransformAction implements ActionExecutor {

    private final Map<String, TransformStrategy> methods = new HashMap<>();

    /**
     * Constructs the transform action and registers all built-in strategy implementations.
     *
     * <p>The strategies are registered with their lowercase method name as the key. The same
     * strategy instance is reused across all invocations because strategies are stateless.
     */
    public TransformAction() {
        methods.put("filter", new FilterStrategy());
        methods.put("select", new SelectStrategy());
        methods.put("map", new MapStrategy());
        methods.put("aggregate", new AggregateStrategy());
        methods.put("derive", new DeriveStrategy());
        methods.put("drop_nulls", new DropNullsStrategy());
        methods.put("fill_nulls", new FillNullsStrategy());
        methods.put("sort", new SortStrategy());
        methods.put("limit", new LimitStrategy());
        methods.put("normalize", new NormalizeStrategy());
        methods.put("scale", new ScaleStrategy());
        methods.put("max", new MaxStrategy());
    }

    /**
     * Dispatches the task to the appropriate strategy based on the method name.
     *
     * <p>Retrieves the current iterator from the context, calls the matching strategy's
     * {@link TransformStrategy#apply} to produce a new (lazy) iterator, and stores the
     * result back on the context. If no strategy matches the method name, a
     * {@link RuntimeException} is thrown.
     *
     * @param ctx the task execution context; its iterator is consumed and replaced
     * @throws RuntimeException if the method name is not registered
     */
    @Override
    public void execute(ExecutionContext ctx) {

        Method method = ctx.getMethod();
        String name = method.getName().toLowerCase();

        TransformStrategy fn = methods.get(name);

        if (fn == null) {
            throw new RuntimeException("Unsupported transform method: " + name);
        }

        DataIterator input = ctx.getIterator();
        DataIterator output = fn.apply(input, method);
        ctx.setIterator(output);
    }

    /**
     * Returns {@code "transform"} – the action type string used in the pipeline XML
     * ({@code <action type="transform">}).
     */
    @Override
    public String getType() {
        return "transform";
    }
}