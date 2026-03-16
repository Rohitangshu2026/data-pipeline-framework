package org.example.datapipeline;

import org.example.datapipeline.cli.Pipeline;

/**
 * Entry point for the data pipeline framework.
 *
 * This class provides the command-line interface for running the
 * pipeline configuration loader. It expects the path to a pipeline
 * XML file as a command-line argument and delegates execution to
 * the Pipeline runner.
 */
public class Main {

    /**
     * Starts the pipeline framework from the command line.
     *
     * The method expects a single argument specifying the path to the
     * pipeline XML configuration file.
     *
     * @param args command-line arguments where args[0] is the pipeline XML path
     * @throws Exception if pipeline parsing or validation fails
     */
    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("Usage: pipeline <pipeline.xml>");
            return;
        }

        Pipeline.run(args[0]);
    }
}