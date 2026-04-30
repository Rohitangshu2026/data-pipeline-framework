package org.example.datapipeline;

import org.example.datapipeline.cli.Pipeline;
import org.example.datapipeline.util.LoggingConfig;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Entry point for the Data Pipeline Framework.
 *
 * <p>Parses command-line arguments and dispatches to one of three operational modes:
 *
 * <ul>
 *   <li><b>Normal execution</b> – {@code pipeline <path-to-pipeline.xml> [--debug|--warn|--info]}
 *       <br>Loads, validates, and runs the pipeline described in the XML file.
 *       Log verbosity can be controlled with an optional flag (default is {@code --info}).</li>
 *   <li><b>Replay</b> – {@code pipeline --replay <run_id>}
 *       <br>Re-executes a previously recorded pipeline run identified by {@code run_id}.
 *       The XML snapshot captured during the original run is written to a temp file and
 *       fed back through {@link org.example.datapipeline.versioning.ReplayService}.</li>
 *   <li><b>List runs</b> – {@code pipeline --list-runs}
 *       <br>Prints all persisted run IDs stored in the {@code runs/} directory.</li>
 * </ul>
 *
 * <p>Logging is initialised before any pipeline work begins via
 * {@link org.example.datapipeline.util.LoggingConfig#setup()}, which wires up a
 * colour-coded console handler and a JSON-formatted file handler that appends to
 * {@code logs/pipeline.log}.
 *
 * <p>All unchecked exceptions thrown during execution are caught here, logged at
 * {@code SEVERE} level, and the JVM exits normally (no {@code System.exit} call is made).
 */
public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    /**
     * Application entry point.
     *
     * <p>Dispatches based on {@code args[0]}:
     * <ul>
     *   <li>{@code --list-runs} – list all saved pipeline run IDs and return.</li>
     *   <li>{@code --replay <run_id>} – replay a previous run and return.</li>
     *   <li>{@code <xml-path>} – run the pipeline described in the given XML file.</li>
     * </ul>
     *
     * <p>An optional second argument controls the JUL log level:
     * {@code --debug} (FINE), {@code --warn} (WARNING), default is INFO.
     *
     * @param args command-line arguments supplied by the JVM
     */
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
