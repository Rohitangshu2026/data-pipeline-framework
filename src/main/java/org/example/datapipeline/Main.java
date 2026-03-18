package org.example.datapipeline;

import org.example.datapipeline.cli.Pipeline;

/**
 * Entry point for the data pipeline framework.
 */
public class Main {

    public static void main(String[] args) {

        if(args.length == 0) {
            System.out.println("Usage: pipeline <pipeline.xml>");
            return;
        }

        try {

            Pipeline.run(args[0]);

        } catch (RuntimeException e) {

            // Handles pipeline semantic errors (cycles, invalid config, etc.)
            System.out.println("Pipeline Error: " + e.getMessage());

        } catch (Exception e) {

            // Handles unexpected errors (parsing, IO)
            System.out.println("Unexpected error occurred:");
            e.printStackTrace();
        }
    }
}
