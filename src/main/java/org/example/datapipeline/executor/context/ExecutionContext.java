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
 * Per-task execution envelope carrying all data an action executor needs at runtime.
 *
 * <p>A new {@code ExecutionContext} is created for each task in
 * {@link org.example.datapipeline.executor.PipelineExecutor#executeStage}. It is never
 * shared across threads — parallel stages each own their own context.
 *
 * <h2>Responsibilities</h2>
 * <ul>
 *   <li><b>Input</b> – exposes the task's {@link org.example.datapipeline.config.input.Input}
 *       so the executor can call {@link org.example.datapipeline.config.input.Input#streamData()}
 *       to obtain the upstream {@link DataIterator}.</li>
 *   <li><b>Output</b> – exposes the task's
 *       {@link org.example.datapipeline.config.output.Output} so the framework can write
 *       the result iterator to the destination file after the action completes.</li>
 *   <li><b>Method</b> – exposes the {@link org.example.datapipeline.config.action.Method}
 *       block so executors can read named parameters without additional lookup.</li>
 *   <li><b>Iterator state</b> – holds the current
 *       {@link org.example.datapipeline.executor.iterator.DataIterator}. The framework sets
 *       the initial input iterator before calling the action; the action replaces it with a
 *       new (possibly chained/transformed) iterator representing the output. The framework
 *       then drains that iterator into the output file.</li>
 *   <li><b>Metadata</b> – a general-purpose {@code Map<String, Object>} used to pass
 *       cross-cutting data (e.g. the global datasource map, stage ID) from the executor
 *       to actions that need it. The join action uses this to resolve {@code right_ref}
 *       datasource IDs dynamically.</li>
 *   <li><b>Temp file registry</b> – tracks temporary files created during execution
 *       (e.g. sort-merge join spill files). All registered files are deleted by
 *       {@link #cleanup()}, which is always called in a {@code finally} block.</li>
 * </ul>
 */
public class ExecutionContext {

    private static final Logger logger = Logger.getLogger(ExecutionContext.class.getName());

    private final Input input;
    private final Output output;
    private final Method method;

    private final Map<String, Object> metadata = new HashMap<>();
    private final List<String> tempFiles = new ArrayList<>();

    private DataIterator iterator;

    /**
     * Creates a new execution context for a single task.
     *
     * @param input  the task's input descriptor (source file/API config); may not be {@code null}
     * @param output the task's output descriptor (destination file config); may be {@code null}
     *               for actions that produce no written output
     * @param method the method configuration block with the operation name and parameters
     */
    public ExecutionContext(Input input, Output output, Method method) {
        this.input = input;
        this.output = output;
        this.method = method;
    }

    /** @return the task's input descriptor */
    public Input getInput()           { return input; }

    /** @return the task's output descriptor, or {@code null} if none declared */
    public Output getOutput()         { return output; }

    /** @return the method configuration block containing the operation name and parameters */
    public Method getMethod()         { return method; }

    /**
     * Returns the mutable metadata map shared between the executor and action implementations.
     *
     * <p>Standard entries placed by {@link org.example.datapipeline.executor.PipelineExecutor}:
     * <ul>
     *   <li>{@code "stageId"} – the current stage's string ID</li>
     *   <li>{@code "globals"} – {@code Map<String, Datasource>} for the entire job</li>
     * </ul>
     *
     * @return mutable metadata map; never {@code null}
     */
    public Map<String, Object> getMetadata() { return metadata; }

    /**
     * Returns the current data iterator.
     *
     * <p>The framework sets this to the input iterator before calling the action. The action
     * replaces it with the output iterator (which may be a lazy-wrapping chain of the input).
     * After the action returns, the framework reads from this iterator to write the output file.
     *
     * @return the current iterator; {@code null} only before the framework sets it
     */
    public DataIterator getIterator() { return iterator; }

    /**
     * Replaces the current data iterator with the given one.
     *
     * <p>Action executors must call this after constructing their output iterator so that
     * the framework can drain it into the output file. Transform strategies delegate to
     * {@link org.example.datapipeline.executor.action.transform.TransformAction}, which
     * calls this method automatically.
     *
     * @param iterator the new iterator; must not be {@code null}
     */
    public void setIterator(DataIterator iterator) { this.iterator = iterator; }

    /**
     * Registers a temporary file path for cleanup after the task completes.
     *
     * <p>Actions that write intermediate spill files (e.g. sort-merge join) should call
     * this method for each file so that {@link #cleanup()} can delete them. This prevents
     * temp files from accumulating across long-running pipelines.
     *
     * @param filePath absolute path of the temp file to delete on cleanup; {@code null}
     *                 is silently ignored
     */
    public void registerTempFile(String filePath) {
        if (filePath != null) tempFiles.add(filePath);
    }

    /**
     * Deletes all registered temporary files and clears the registry.
     *
     * <p>Called in a {@code finally} block by
     * {@link org.example.datapipeline.executor.PipelineExecutor#executeStage} to ensure temp
     * files are removed even if the task fails. Deletion failures are logged at WARNING level
     * but do not propagate as exceptions.
     */
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