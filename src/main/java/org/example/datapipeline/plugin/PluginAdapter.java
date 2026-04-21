package org.example.datapipeline.plugin;

import org.example.datapipeline.executor.action.ActionExecutor;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

public class PluginAdapter implements ActionExecutor {

    private final ActionPlugin plugin;

    public PluginAdapter(ActionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void execute(ExecutionContext ctx) {
        DataIterator newIterator = plugin.getExecutor().execute(ctx);
        ctx.setIterator(newIterator);
    }

    @Override
    public String getType() {
        return plugin.getType().toLowerCase();
    }
}
