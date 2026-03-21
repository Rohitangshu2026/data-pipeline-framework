package org.example.datapipeline.executor.context;

import org.example.datapipeline.config.input.Input;
import org.example.datapipeline.config.output.Output;
import org.example.datapipeline.config.action.Method;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates all data required during the execution of a pipeline task.
 *
 * This context object carries the input source, output destination, and
 * method configuration associated with a task. It also provides a flexible
 * metadata map that can be used to store and share additional information
 * across different components during execution (such as stage identifiers,
 * runtime flags, or intermediate results).
 *
 * By bundling execution-related data into a single object, this class
 * simplifies method signatures and promotes cleaner, more extensible
 * action implementations.
 */
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

    public Input getInput(){
        return input;
    }

    public Output getOutput(){
        return output;
    }

    public Method getMethod(){
        return method;
    }

    public Map<String, Object> getMetadata(){
        return metadata;
    }
}