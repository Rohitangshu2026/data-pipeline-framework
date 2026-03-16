package org.example.datapipeline.executor;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.config.Task;

import java.util.List;

public class PipelineExecutor {

    public static void execute(Job job) {

        List<List<Stage>> levels = job.getExecutionLevels();

        System.out.println("\n----- PIPELINE EXECUTION START -----");

        for (int level = 0; level < levels.size(); level++) {

            System.out.println("\nExecuting Stage Level: " + level);

            for (Stage stage : levels.get(level)) {

                executeStage(stage);
            }
        }

        System.out.println("\n----- PIPELINE EXECUTION COMPLETE -----");
    }

    private static void executeStage(Stage stage) {

        System.out.println("\nStage: " + stage.getId());

        for (Task task : stage.getTasks()) {

            String action = task.getAction().getType();

            System.out.println("Executing Task:");
            System.out.println("  Input: " + task.getInput().getSrc());
            System.out.println("  Action: " + action);
            System.out.println("  Output: " + task.getOutput().getSrc());
        }
    }
}
