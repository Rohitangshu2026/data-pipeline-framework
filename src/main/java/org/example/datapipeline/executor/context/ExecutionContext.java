package org.example.datapipeline.executor.context;

import org.example.datapipeline.config.input.Input;
import org.example.datapipeline.config.output.Output;
import org.example.datapipeline.config.action.Method;

import java.util.HashMap;
import java.util.Map;

public class ExecutionContext {

    private Input input;
    private Output output;
    private Method method;

    private Map<String, Object> metadata = new HashMap<>();

    public ExecutionContext(Input input, Output output, Method method) {
        this.input = input;
        this.output = output;
        this.method = method;
    }

    public Input getInput() { return input; }
    public Output getOutput() { return output; }
    public Method getMethod() { return method; }

    public Map<String, Object> getMetadata() { return metadata; }
}