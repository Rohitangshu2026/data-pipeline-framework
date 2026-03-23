package org.example.datapipeline.executor;

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

/**
 * Represents the execution context for a pipeline task.
 *
 * Holds all information required during task execution, including
 * input source, output destination, method configuration, and
 * intermediate data.
 *
 * Provides a shared data container used across ETL stages:
 * - input  : source of data
 * - data   : in-memory dataset processed by transforms
 * - output : destination for final results
 *
 * Also includes a flexible metadata map for passing additional
 * information such as stage identifiers or runtime context.
 *
 * This class enables decoupled and extensible action execution
 * by centralizing all runtime state in a single object.
 */
public class PipelineExecutor {

    private static final Logger logger = Logger.getLogger(PipelineExecutor.class.getName());
    public static void execute(Job job) {

        List<List<Stage>> levels = job.getExecutionLevels();

        logger.info("PIPELINE_EXECUTION_START");

        for (int level = 0; level < levels.size(); level++) {

            logger.info("STAGE_LEVEL_START level=" + level);

            levels.get(level)
                    .parallelStream()
                    .forEach(PipelineExecutor::executeStage);
        }
    }

    private static void executeStage(Stage stage) {
        long stageStart = System.currentTimeMillis();
        int maxRetries = 1; // user requested 1 retry
        int currentAttempt = 0;
        
        org.example.datapipeline.config.OnError onError = stage.getOnError();
        String strategy = (onError != null && onError.getHandlingStrategy() != null) 
                ? onError.getHandlingStrategy() : "abort";

        while (true) {
            try {
                for (Task task : stage.getTasks()) {

                    String methodName = task.getAction().getMethod().getName();
                    logger.info(String.format(
                            "TASK_START stage=%s input=%s action=%s method=%s output=%s",
                            stage.getId(),
                            task.getInput().getSrc(),
                            task.getAction().getType(),
                            methodName,
                            task.getOutput().getSrc()
                    ));

                    ExecutionContext ctx = new ExecutionContext(
                            task.getInput(),
                            task.getOutput(),
                            task.getAction().getMethod()
                    );

                    ctx.getMetadata().put("stageId", stage.getId());

                    TaskMetrics metrics = new TaskMetrics();
                    metrics.start();

                    CountingIterator inputIt = null;
                    CountingIterator outputIt = null;

                    try {
                        // Wrap input
                        inputIt = new CountingIterator(task.getInput().streamData());
                        ctx.setIterator(inputIt);

                        ActionExecutor executor = ActionRegistry.getAction(
                                task.getAction().getType()
                        );

                        executor.execute(ctx);

                        // Wrap output
                        outputIt = new CountingIterator(ctx.getIterator());
                        task.getOutput().writeData(outputIt);

                        metrics.setRowsIn(inputIt.getCount());
                        metrics.setRowsOut(outputIt.getCount());
                        metrics.setSuccess(true);

                    } catch (Exception e) {

                        metrics.setSuccess(false);
                        metrics.setError(e.getMessage());
                        throw e;

                    } finally {

                        metrics.end();

                        logger.info(String.format(
                                "TASK_METRICS stage=%s method=%s duration=%d rowsIn=%d rowsOut=%d success=%s error=%s",
                                stage.getId(),
                                methodName,
                                metrics.getDuration(),
                                inputIt != null ? inputIt.getCount() : 0,
                                outputIt != null ? outputIt.getCount() : 0,
                                metrics.isSuccess(),
                                metrics.isSuccess() ? "none" : metrics.getError()
                        ));
                    }

                }
                break; // If successful, exit the retry loop
            } catch (Exception e) {
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

        logger.info(String.format(
                "STAGE_METRICS stage=%s duration=%d retries=%d",
                stage.getId(),
                (stageEnd - stageStart),
                currentAttempt
        ));
    }
}
