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

public class PipelineExecutor {

    private static final Logger logger = Logger.getLogger(PipelineExecutor.class.getName());

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

    public static void execute(Job job) {
        execute(job, "<snapshot_unavailable>");
    }

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
