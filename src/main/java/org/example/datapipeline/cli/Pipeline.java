package org.example.datapipeline.cli;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Task;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.parser.JAXBPipelineParser;
import org.example.datapipeline.validator.SemanticValidator;
import org.example.datapipeline.util.ConfigNormalizer;

/**
 * CLI runner responsible for loading and validating a pipeline configuration.
 *
 * The execution flow is:
 * 1. Parse the XML pipeline configuration
 * 2. Perform semantic validation
 * 3. Normalize configuration fields
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
        try{
            JAXBPipelineParser parser = new JAXBPipelineParser();
            Job job = parser.parse(xmlPath);
            SemanticValidator.validate(job);
            ConfigNormalizer.normalize(job);
            System.out.println("Pipeline loaded: " + job.getId());
            System.out.println("Stages: " + job.getStages().size());

            // ---- DEBUG PRINT ----
            for(Stage stage : job.getStages()) {
                System.out.println("Stage: " + stage.getId());
                System.out.println("Dependencies: " + stage.getDependencies());
                for(Task task : stage.getTasks()) {
                    System.out.println("  Task:");
                    System.out.println("    Input: " + task.getInput().getSrc());
                    System.out.println("    Action: " + task.getAction().getType());
                    System.out.println("    Output: " + task.getOutput().getSrc());
                }
            }

        }
        catch (Exception e) {
            System.out.println("Pipeline configuration error\n");
            Throwable cause = e.getCause();
            if (cause instanceof org.xml.sax.SAXParseException sax) {
                System.out.println("Line: " + sax.getLineNumber());
                System.out.println("Column: " + sax.getColumnNumber());
                System.out.println("Error: " + sax.getMessage());
            }
            else {
                System.out.println("Error: " + e.getMessage());
            }
            System.exit(1);
        }

    }

}