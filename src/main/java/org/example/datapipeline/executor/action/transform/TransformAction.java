package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;


import java.util.HashMap;
import java.util.Map;

/**
 * Executes transformation actions by delegating to specific transform methods.
 *
 * Maintains a registry of supported transformations and dynamically resolves
 * the appropriate implementation based on the method name defined in the
 * pipeline configuration.
 *
 * Supported transform methods:
 * - filter    : filters rows based on a condition
 * - select    : projects a subset of columns
 * - map       : applies arithmetic operations to a column
 * - aggregate : performs grouped aggregations
 *
 * Each transform operates on a DataIterator, enabling streaming execution.
 * The input iterator is passed to the selected transform, and the resulting
 * iterator is set back into the execution context for downstream processing.
 *
 * This design allows transforms to be chained efficiently without materializing
 * intermediate datasets in memory, making it suitable for large-scale data processing.
 *
 * New transform methods can be added by registering them in the internal map
 * without modifying the execution flow.
 */
public class TransformAction implements ActionExecutor {

    private final Map<String, TransformMethod> methods = new HashMap<>();

    public TransformAction() {
        methods.put("filter", new FilterTransform());
        methods.put("select", new SelectTransform());
        methods.put("map", new MapTransform());
        methods.put("aggregate", new AggregateTransform());
    }

    @Override
    public void execute(ExecutionContext ctx) {

        Method method = ctx.getMethod();
        String name = method.getName().toLowerCase();

        TransformMethod fn = methods.get(name);

        if (fn == null) {
            throw new RuntimeException("Unsupported transform method: " + name);
        }

        DataIterator input = ctx.getIterator();
        DataIterator output = fn.apply(input, ctx);
        ctx.setIterator(output);
    }

    @Override
    public String getType() {
        return "transform";
    }
}