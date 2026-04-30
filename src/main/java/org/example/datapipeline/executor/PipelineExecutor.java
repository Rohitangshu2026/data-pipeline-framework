package org.example.datapipeline.executor;

import org.example.datapipeline.config.Datasource;
import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.config.Task;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;
import org.example.datapipeline.executor.action.ActionRegistry;
import org.example.datapipeline.executor.metrics.TaskMetrics;
import org.example.datapipeline.executor.metrics.CountingIterator;
import java.util.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.example.datapipeline.versioning.*;

/**
 * Core execution engine that runs a validated {@link Job} to completion.
 *
 * <h2>Execution Model</h2>
 * <p>The executor applies Kahn's topological-sort output (produced by
 * {@link Job#getExecutionLevels()}) level by level. Within each level, all stages are
 * dispatched concurrently via {@code parallelStream}. Stages within the same level share
 * no data dependencies and can run in true parallel (bound by the JVM's common fork-join
 * pool thread count). Stages across levels are serialised – level N+1 never begins until
 * every stage in level N has completed or been skipped.
 *
 * <h2>Stage Execution</h2>
 * <p>For each stage, {@link #executeStage} runs its tasks sequentially in declaration order.
 * Each task goes through the following lifecycle:
 * <ol>
 *   <li>Resolve any remaining datasource references (needed for stages whose tasks declared
 *       inline I/O rather than global {@code ref} attributes).</li>
 *   <li>Open the input as a {@link org.example.datapipeline.executor.metrics.CountingIterator}
 *       wrapping the underlying {@link org.example.datapipeline.executor.iterator.DataIterator}.</li>
 *   <li>Run the action executor ({@link org.example.datapipeline.executor.action.ActionExecutor#execute}).</li>
 *   <li>Drain the output iterator through a second {@code CountingIterator} — either by
 *       writing rows to the output file (normal case) or by consuming without writing
 *       (when {@link org.example.datapipeline.executor.action.ActionExecutor#handlesOwnOutput()}
 *       returns {@code true}, as with {@link org.example.datapipeline.executor.action.BashAction}).</li>
 *   <li>Record row counts, duration, and success/failure in a
 *       {@link org.example.datapipeline.versioning.TaskRun} appended to the pipeline run record.</li>
 * </ol>
 *
 * <h2>Error Handling</h2>
 * <p>Each stage carries an optional {@link org.example.datapipeline.config.OnError} configuration
 * with three strategies:
 * <ul>
 *   <li>{@code abort} (default) – re-throws the exception, which propagates out of
 *       {@code parallelStream} and aborts the pipeline.</li>
 *   <li>{@code proceed} – logs a warning and continues to the next stage; the failed
 *       stage's output file is left in whatever state the failure left it.</li>
 *   <li>{@code retry} – re-executes the stage up to {@code retry_count} times before
 *       aborting. Each attempt resets the stage status to {@code RUNNING}.</li>
 * </ul>
 *
 * <h2>Run Recording</h2>
 * <p>Every execution creates a {@link org.example.datapipeline.versioning.PipelineRun} UUID,
 * captures the raw XML snapshot, and records per-stage and per-task metrics. The run is
 * saved to the {@code runs/} directory in JSON format by
 * {@link org.example.datapipeline.versioning.JsonPipelineRunManager} in the {@code finally}
 * block, so it is persisted regardless of success or failure. The run ID can be passed to
 * {@link org.example.datapipeline.versioning.ReplayService} to replay the exact pipeline.
 */
public class PipelineExecutor {

    private static final Logger logger = Logger.getLogger(PipelineExecutor.class.getName());

    /**
     * Executes a validated pipeline job, capturing the XML snapshot for replay.
     *
     * <p>Creates a new {@link org.example.datapipeline.versioning.PipelineRun}, builds the
     * stage lookup map, computes execution levels, iterates through each level in order,
     * and dispatches stages within each level in parallel. The run record is saved in the
     * {@code finally} block to ensure persistence even on failure.
     *
     * @param job         the validated and normalised pipeline job
     * @param xmlSnapshot the raw XML content used to create this run (stored for replay)
     * @throws RuntimeException if any stage aborts due to an unrecovered error
     */
    public static void execute(Job job, String xmlSnapshot) {
        job.buildStageMap();

        List<List<Stage>> levels = job.getExecutionLevels();

        logger.info("PIPELINE_EXECUTION_START");

        PipelineRunManager runManager = new JsonPipelineRunManager();
        PipelineRun run = new PipelineRun(UUID.randomUUID().toString());
        run.setXmlSnapshot(xmlSnapshot);
        run.setStartTime(System.currentTimeMillis());
        run.setStatus("RUNNING");

        try {
            for (int level = 0; level < levels.size(); level++) {
                logger.info("STAGE_LEVEL_START level=" + level);
                levels.get(level)
                        .parallelStream()
                        .forEach(stage -> executeStage(stage, job.getDatasourceMap(), run));
            }
            run.setStatus("SUCCESS");
        } catch (Exception e) {
            run.setStatus("FAILED");
            throw e;
        } finally {
            run.setEndTime(System.currentTimeMillis());
            runManager.saveRun(run);
        }
    }

    /**
     * Convenience overload for executing a job without an XML snapshot.
     *
     * <p>Used internally for testing or when the XML content is unavailable. The run record
     * will contain the placeholder string {@code "<snapshot_unavailable>"} instead of the
     * actual XML, which means replay is not possible for runs created this way.
     *
     * @param job the validated and normalised pipeline job
     * @throws RuntimeException if any stage aborts due to an unrecovered error
     */
    public static void execute(Job job) {
        execute(job, "<snapshot_unavailable>");
    }

    /**
     * Executes all tasks in a single stage, applying the configured error-handling strategy.
     *
     * <p>Tasks within the stage run sequentially in declaration order. If any task throws an
     * exception, the stage's {@link org.example.datapipeline.config.OnError} strategy
     * determines the response: abort (re-throw), proceed (log and continue), or retry
     * (re-execute the stage from the beginning, up to {@code retry_count} times).
     *
     * <p>Stage and task metrics ({@link org.example.datapipeline.versioning.StageRun},
     * {@link org.example.datapipeline.versioning.TaskRun}) are added to the {@code PipelineRun}
     * inside a {@code synchronized} block to prevent data races when multiple stages at the
     * same level run concurrently.
     *
     * @param stage       the stage to execute
     * @param globals     the job-level datasource map, passed through the execution context
     * @param pipelineRun the run record to which stage and task metrics are appended
     */
    private static void executeStage(Stage stage, Map<String, Datasource> globals, PipelineRun pipelineRun) {
        long stageStart = System.currentTimeMillis();
        // int maxRetries = 1;
        int currentAttempt = 0;

        StageRun stageRun = new StageRun(stage.getId());
        stageRun.setStartTime(stageStart);
        stageRun.setStatus("RUNNING");
        synchronized (pipelineRun) {
            pipelineRun.addStage(stageRun);
        }

        org.example.datapipeline.config.OnError onError = stage.getOnError();
        String strategy = (onError != null && onError.getHandlingStrategy() != null)
                ? onError.getHandlingStrategy()
                : "abort";
        int maxRetries = (onError != null && onError.getRetryCount() != null)
                ? onError.getRetryCount()
                : 1;

        while (true) {
            stageRun.setStatus("RUNNING");
            try {
                for (Task task : stage.getTasks()) {
                    TaskRun taskRun = new TaskRun(task.getAction().getType());
                    long taskStart = System.currentTimeMillis();

                    if (task.getInput() != null)
                        task.getInput().resolve(globals);
                    if (task.getOutput() != null)
                        task.getOutput().resolve(globals);

                    String methodName = task.getAction().getMethod().getName();
                    logger.info(String.format(
                            "TASK_START stage=%s input=%s action=%s method=%s output=%s",
                            stage.getId(),
                            task.getInput() != null ? task.getInput().getSrc() : "none",
                            task.getAction().getType(),
                            methodName,
                            task.getOutput() != null ? task.getOutput().getSrc() : "none"));

                    ExecutionContext ctx = new ExecutionContext(
                            task.getInput(),
                            task.getOutput(),
                            task.getAction().getMethod());

                    ctx.getMetadata().put("stageId", stage.getId());
                    ctx.getMetadata().put("globals", globals);

                    TaskMetrics metrics = new TaskMetrics();
                    metrics.start();

                    CountingIterator inputIt = null;
                    CountingIterator outputIt = null;

                    try {
                        inputIt = new CountingIterator(task.getInput().streamData());
                        ctx.setIterator(inputIt);

                        ActionExecutor executor = ActionRegistry.getAction(
                                task.getAction().getType());

                        executor.execute(ctx);

                        outputIt = new CountingIterator(ctx.getIterator());
                        if (executor.handlesOwnOutput()) {
                            // Action wrote its own output (e.g. bash script); drain
                            // the iterator only for row-count metrics — do NOT writeData.
                            while (outputIt.hasNext()) outputIt.next();
                        } else {
                            task.getOutput().writeData(outputIt);
                        }

                        metrics.setRowsIn(inputIt.getCount());
                        metrics.setRowsOut(outputIt.getCount());
                        metrics.setSuccess(true);
                        
                        taskRun.setRowsIn(inputIt.getCount());
                        taskRun.setRowsOut(outputIt.getCount());
                        taskRun.setSuccess(true);

                    } catch (Exception e) {
                        metrics.setSuccess(false);
                        metrics.setError(e.getMessage());
                        
                        taskRun.setSuccess(false);
                        taskRun.setError(e.getMessage());
                        
                        throw e;
                    } finally {
                        ctx.cleanup();
                        metrics.end();
                        taskRun.setDuration(System.currentTimeMillis() - taskStart);
                        stageRun.addTask(taskRun);
                        logger.info(String.format(
                                "TASK_METRICS stage=%s method=%s duration=%d rowsIn=%d rowsOut=%d success=%s error=%s",
                                stage.getId(),
                                methodName,
                                metrics.getDuration(),
                                inputIt != null ? inputIt.getCount() : 0,
                                outputIt != null ? outputIt.getCount() : 0,
                                metrics.isSuccess(),
                                metrics.isSuccess() ? "none" : metrics.getError()));
                    }
                }
                stageRun.setStatus("SUCCESS");
                break;
            } catch (Exception e) {
                stageRun.setStatus("FAILED");
                if ("proceed".equals(strategy)) {
                    logger.warning("STAGE_SKIPPED stage=" + stage.getId());
                    break;
                } else if ("retry".equals(strategy)) {
                    currentAttempt++;
                    if (currentAttempt <= maxRetries) {
                        logger.warning("RETRY stage=" + stage.getId() + " attempt=" + currentAttempt);
                    } else {
                        logger.severe("STAGE_ABORT stage=" + stage.getId() + " error=" + e.getMessage());
                        throw new RuntimeException("aborted due to error at stage " + stage.getId(), e);
                    }
                } else {
                    logger.severe("STAGE_ABORT stage=" + stage.getId() + " error=" + e.getMessage());
                    throw new RuntimeException("aborted due to error at stage " + stage.getId(), e);
                }
            }
        }
        long stageEnd = System.currentTimeMillis();
        stageRun.setEndTime(stageEnd);

        logger.info(String.format(
                "STAGE_METRICS stage=%s duration=%d retries=%d",
                stage.getId(),
                (stageEnd - stageStart),
                currentAttempt));
    }
}
