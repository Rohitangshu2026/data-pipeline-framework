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
            return;
        }
        String xmlPath = args[0];
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
            logger.info("PIPELINE_START file=" + xmlPath);
            Pipeline.run(xmlPath);
            logger.info("PIPELINE_EXECUTION_END");
            
        } catch (RuntimeException e) {

            logger.severe("PIPELINE_ERROR " + e.getMessage());

        } catch (Exception e) {

            logger.severe("UNEXPECTED_ERROR " + e.getMessage());
            e.printStackTrace();
        }
    }
}
