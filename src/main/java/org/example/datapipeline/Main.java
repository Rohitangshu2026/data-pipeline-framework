package org.example.datapipeline;

import org.example.datapipeline.cli.Pipeline;
import org.example.datapipeline.util.LoggingConfig;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Entry point for the data pipeline framework.
 */
public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        if(args.length == 0) {
            System.out.println("Usage: pipeline <pipeline.xml> [--debug|--warn]");
            System.out.println("       pipeline --replay <run_id>");
            System.out.println("       pipeline --list-runs");
            return;
        }

        if ("--list-runs".equals(args[0])) {
            org.example.datapipeline.versioning.PipelineRunManager manager = new org.example.datapipeline.versioning.JsonPipelineRunManager();
            java.util.List<String> runs = manager.listRuns();
            System.out.println("Available runs:");
            for (String r : runs) {
                System.out.println(" - " + r);
            }
            return;
        }

        if (args.length >= 2 && "--replay".equals(args[0])) {
            String runId = args[1];
            LoggingConfig.setup();
            logger.info("REPLAY_START run_id=" + runId);
            try {
                org.example.datapipeline.versioning.ReplayService.replay(runId);
            } catch (Exception e) {
                logger.severe("UNEXPECTED_ERROR " + e.getMessage());
                e.printStackTrace();
            }
            return;
        }

        String arg0 = args[0];
        String logLevel = args.length > 1 ? args[1] : "--info";

        // Setup logging
        LoggingConfig.setup();

        // Set log level
        switch (logLevel) {
            case "--debug":
                Logger.getLogger("").setLevel(Level.FINE);
                break;
            case "--warn":
                Logger.getLogger("").setLevel(Level.WARNING);
                break;
            default:
                Logger.getLogger("").setLevel(Level.INFO);
        }
        try {
            logger.info("PIPELINE_START file=" + arg0);
            Pipeline.run(arg0);
            logger.info("PIPELINE_EXECUTION_END");
            
        } catch (RuntimeException e) {

            logger.severe("PIPELINE_ERROR " + e.getMessage());

        } catch (Exception e) {

            logger.severe("UNEXPECTED_ERROR " + e.getMessage());
            e.printStackTrace();
        }
    }
}
