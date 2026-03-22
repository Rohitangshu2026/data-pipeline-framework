package org.example.datapipeline.executor;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.config.Task;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.action.ActionExecutor;
import org.example.datapipeline.executor.action.ActionRegistry;
import org.example.datapipeline.executor.iterator.DataIterator;

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


            String log = String.format(
                    "\n[%s] TASK START\nInput: %s\nAction: %s\nMethod: %s\nOutput: %s\n",
                    stage.getId(),
                    task.getInput().getSrc(),
                    task.getAction().getType(),
                    task.getAction().getMethod().getName(),
                    task.getOutput().getSrc()
            );
            System.out.println(log);

            ExecutionContext ctx = new ExecutionContext(
                    task.getInput(),
                    task.getOutput(),
                    task.getAction().getMethod()
            );

            ctx.getMetadata().put("stageId", stage.getId());

            DataIterator it = task.getInput().streamData();
            ctx.setIterator(it);

            ActionExecutor executor = ActionRegistry.getAction(
                    task.getAction().getType()
            );
            executor.execute(ctx);

            DataIterator result = ctx.getIterator();
            task.getOutput().writeData(result);

        }
    }
}
