package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;

import java.util.HashMap;
import java.util.Map;

public class TransformAction implements ActionExecutor {

    private final Map<String, TransformStrategy> methods = new HashMap<>();

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
    }

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

    @Override
    public String getType() {
        return "transform";
    }
}