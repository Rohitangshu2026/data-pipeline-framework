package org.example.datapipeline.executor;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.config.Task;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;
import org.example.datapipeline.executor.action.ActionRegistry;

import java.util.List;

/**
 * Orchestrates the execution of a pipeline job.
 *
 * This executor coordinates the execution of stages and tasks defined
 * in a job configuration. It processes the pipeline in levels, where
 * each level contains stages that can be executed in parallel, while
 * maintaining sequential execution across levels.
 *
 * Features:
 * - Level-based Execution:
 *   Stages are grouped into execution levels, ensuring dependency-aware
 *   ordering. Each level is executed sequentially.
 *
 * - Parallel Stage Execution:
 *   Stages within the same level are executed concurrently to improve
 *   performance.
 *
 * - Task Execution:
 *   Each stage executes its tasks sequentially. For every task, the
 *   appropriate action executor is resolved dynamically and invoked
 *   using a shared execution context.
 *
 * - Context Propagation:
 *   Execution context carries input, output, method configuration, and
 *   metadata (such as stage identifiers) across the execution flow.
 *
 * - Logging:
 *   Provides structured logs for tracking stage levels, task execution,
 *   and pipeline progress.
 *
 * The design is extensible, allowing new action types, execution
 * strategies, or metadata enhancements without changing the core
 * orchestration logic.
 */
public class PipelineExecutor {

    public static void execute(Job job) {

        List<List<Stage>> levels = job.getExecutionLevels();

        System.out.println("\n----- PIPELINE EXECUTION START -----");

        for (int level = 0; level < levels.size(); level++) {

            System.out.println("\nExecuting Stage Level: " + level);

            levels.get(level)
                    .parallelStream()
                    .forEach(PipelineExecutor::executeStage);
        }

        System.out.println("\n----- PIPELINE EXECUTION COMPLETE -----");
    }

    private static void executeStage(Stage stage) {
    //        System.out.println("\nStage: " + stage.getId());

        for (Task task : stage.getTasks()) {

            String action = task.getAction().getType();

            String log = String.format(
                    "\n[%s] TASK START\nInput: %s\nAction: %s\nMethod: %s\nOutput: %s\n",
                    stage.getId(),
                    task.getInput().getSrc(),
                    task.getAction().getType(),
                    task.getAction().getMethod().getName(),  // ✅ method name
                    task.getOutput().getSrc()
            );
            System.out.println(log);

            ExecutionContext ctx = new ExecutionContext(
                    task.getInput(),
                    task.getOutput(),
                    task.getAction().getMethod()
            );

            ctx.getMetadata().put("stageId", stage.getId());

            ActionExecutor executor = ActionRegistry.getAction(
                    task.getAction().getType()
            );

            executor.execute(ctx);
        }
    }
}
