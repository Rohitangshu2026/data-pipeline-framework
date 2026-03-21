package org.example.datapipeline.executor.action;

import org.example.datapipeline.config.input.Input;
import org.example.datapipeline.config.output.Output;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;


public interface ActionExecutor {
    void execute(ExecutionContext ctx);
    String getType();
}