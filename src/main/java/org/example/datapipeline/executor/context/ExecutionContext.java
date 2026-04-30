package org.example.datapipeline.executor.context;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.config.input.Input;
import org.example.datapipeline.config.output.Output;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Encapsulates all data required during the execution of a pipeline task.
 *
 * Carries input, output, method config, and a streaming DataIterator.
 * Also manages temp files created during execution (e.g. sort-merge join
 * spill files), deleting them on cleanup().
 *
 * One ExecutionContext is created per task and never shared across threads.
 */
public class ExecutionContext {

    private static final Logger logger = Logger.getLogger(ExecutionContext.class.getName());

    private final Input input;
    private final Output output;
    private final Method method;

    private final Map<String, Object> metadata = new HashMap<>();
    private final List<String> tempFiles = new ArrayList<>();

    private DataIterator iterator;

    public ExecutionContext(Input input, Output output, Method method) {
        this.input = input;
        this.output = output;
        this.method = method;
    }

    public Input getInput()           { return input; }
    public Output getOutput()         { return output; }
    public Method getMethod()         { return method; }
    public Map<String, Object> getMetadata() { return metadata; }

    public DataIterator getIterator() { return iterator; }
    public void setIterator(DataIterator iterator) { this.iterator = iterator; }

    public void registerTempFile(String filePath) {
        if (filePath != null) tempFiles.add(filePath);
    }

    public void cleanup() {
        for (String filePath : tempFiles) {
            File file = new File(filePath);
            if (file.exists() && !file.delete()) {
                logger.warning("TEMP_FILE_DELETE_FAILED path=" + filePath);
            }
        }
        tempFiles.clear();
    }
}