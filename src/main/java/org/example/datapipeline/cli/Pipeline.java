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
 * Orchestrates the full lifecycle of a pipeline execution from a single XML file path.
 *
 * <p>This class acts as the primary facade between the CLI entry point ({@link org.example.datapipeline.Main})
 * and the internal execution engine ({@link org.example.datapipeline.executor.PipelineExecutor}).
 * It performs all pre-execution steps in order:
 *
 * <ol>
 *   <li><b>Parse</b> – deserialise the XML file into a {@link org.example.datapipeline.config.Job}
 *       object graph using JAXB, validated against the bundled XSD schema.</li>
 *   <li><b>Resolve datasources</b> – replace symbolic {@code ref} attributes in task inputs/outputs
 *       with the concrete parameters from the global {@code <datasources>} block.</li>
 *   <li><b>Semantic validation</b> – verify stage IDs are unique, all dependencies refer to
 *       existing stages, every task has input/action/output, and {@code on_error} configs are
 *       internally consistent.</li>
 *   <li><b>Normalize</b> – convert whitespace-delimited {@code pre_req} strings into typed
 *       dependency sets and build the stage lookup map.</li>
 *   <li><b>Compute execution levels</b> – run Kahn's BFS topological sort to group stages into
 *       parallel execution levels. Each level is a batch of stages that can run concurrently
 *       because all their predecessors have already completed.</li>
 *   <li><b>Execute</b> – hand the validated {@link org.example.datapipeline.config.Job} and
 *       the raw XML snapshot to {@link org.example.datapipeline.executor.PipelineExecutor},
 *       which processes each level in order using {@code parallelStream}.</li>
 * </ol>
 *
 * <p>A human-readable summary of stages and their topological level order is printed to
 * {@code System.out} before execution begins, making it easy to audit the DAG without
 * reading the XML.
 */
public class Pipeline {

    /**
     * Loads, validates, and runs the pipeline described in the given XML file.
     *
     * <p>The raw XML content is also read as a string and passed to the executor so that
     * it can be captured in the run record and used for future replays.
     *
     * @param xmlPath path to the pipeline XML configuration file (relative or absolute)
     * @throws Exception if any step – parsing, validation, normalisation, or execution – fails
     */
    public static void run(String xmlPath) throws Exception {

        JAXBPipelineParser parser = new JAXBPipelineParser();

        Job job = parser.parse(xmlPath);
        job.resolveDatasources();
        SemanticValidator.validate(job);

        ConfigNormalizer.normalize(job);
        System.out.println("Pipeline loaded: " + job.getId());
        System.out.println("Stages: " + job.getStages().size());

        System.out.println("\n----- PIPELINE STAGES -----");
        for (Stage stage : job.getStages()) {

            System.out.println("\nStage: " + stage.getId());
            System.out.println("Dependencies: " + stage.getDependencies());

            for (Task task : stage.getTasks()) {
                System.out.println("  Task:");
                System.out.println("    Input: " + task.getInput().getSrc());
                System.out.println("    Action: " + task.getAction().getType());
                System.out.println("    Output: " + task.getOutput().getSrc());

            }
        }
        List<List<Stage>> levels = job.getExecutionLevels();

        System.out.println("\n---- TOPOLOGICAL LEVEL ORDER ----\n");

        for (List<Stage> level : levels) {
            System.out.println("Level " + levels.indexOf(level) + ": " + level.stream().map(Stage::getId).toList());
        }

        String xmlSnapshot = java.nio.file.Files.readString(java.nio.file.Paths.get(xmlPath), java.nio.charset.StandardCharsets.UTF_8);
        PipelineExecutor.execute(job, xmlSnapshot);
    }
}