package org.example.datapipeline.cli;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Task;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.executor.PipelineExecutor;
import org.example.datapipeline.parser.JAXBPipelineParser;
import org.example.datapipeline.validator.SemanticValidator;
import org.example.datapipeline.util.ConfigNormalizer;

import java.util.*;

/**
 * CLI runner responsible for loading and validating a pipeline configuration.
 *
 * The execution flow is:
 * 1. Parse the XML pipeline configuration
 * 2. Perform semantic validation
 * 3. Normalize configuration fields (e.g., resolve stage dependencies)
 *
 * This produces a validated Job configuration is
 * passed to the DAG builder and execution engine.
 */
public class Pipeline {

    /**
     * Runs the pipeline configuration loader.
     *
     * @param xmlPath path to the pipeline XML configuration file
     * @throws Exception if parsing or validation fails
     */
    public static void run(String xmlPath) throws Exception {

        JAXBPipelineParser parser = new JAXBPipelineParser();

        Job job = parser.parse(xmlPath);
        SemanticValidator.validate(job);

        ConfigNormalizer.normalize(job);
        System.out.println("Pipeline loaded: " + job.getId());
        System.out.println("Stages: " + job.getStages().size());

        System.out.println("\n----- PIPELINE STAGES -----");
        for(Stage stage : job.getStages()) {

            System.out.println("\nStage: " + stage.getId());
            System.out.println("Dependencies: " + stage.getDependencies());

            for(Task task : stage.getTasks()) {
                System.out.println("  Task:");
                System.out.println("    Input: " + task.getInput().getSrc());
                System.out.println("    Action: " + task.getAction().getType());
                System.out.println("    Output: " + task.getOutput().getSrc());

            }
        }
        List<List<Stage>> levels = job.getExecutionLevels();

        System.out.println("\n---- TOPOLOGICAL LEVEL ORDER ----\n");

        for(List<Stage> level : levels) {
            System.out.println("Level " + levels.indexOf(level) + ": " + level.stream().map(Stage::getId).toList());
        }

        System.out.println("Dummy Execution\n");
        PipelineExecutor.execute(job);
    }


}