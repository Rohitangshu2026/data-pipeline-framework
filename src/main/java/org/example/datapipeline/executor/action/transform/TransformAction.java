package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.action.transform.*;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;


import java.util.HashMap;
import java.util.Map;

/**
 * Executes transformation actions within the pipeline by delegating
 * to specific transform methods.
 *
 * Maintains a registry of supported transformation operations and
 * dynamically resolves the appropriate implementation based on the
 * method name provided in the configuration.
 *
 * Supported transform methods include:
 * - filter    : filters rows based on a condition
 * - select    : selects a subset of columns
 * - map       : applies arithmetic operations to a column
 * - aggregate : performs grouped aggregations on data
 *
 * The execution context provides input data, method parameters,
 * and a shared data structure that is modified in place by each transform.
 *
 * This design enables extensibility by allowing new transform methods
 * to be added without modifying the execution flow.
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

        fn.apply(ctx);
    }

    @Override
    public String getType() {
        return "transform";
    }
}