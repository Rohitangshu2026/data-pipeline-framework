package org.example.datapipeline.versioning;

import org.example.datapipeline.cli.Pipeline;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.UUID;

/**
 * Service that re-executes a pipeline using the exact configuration from a previous run.
 *
 * <p>Every pipeline execution captures a snapshot of the raw XML configuration inside its
 * {@link PipelineRun} record. {@code ReplayService} retrieves that snapshot, writes it to
 * a temporary file in {@code /tmp/}, and passes it through the full pipeline lifecycle:
 * parse → validate → normalise → execute. This guarantees that the replay uses precisely
 * the same configuration as the original run, regardless of any subsequent changes to the
 * source XML files.
 *
 * <h2>Use Cases</h2>
 * <ul>
 *   <li><b>Debugging</b> – reproduce a failed run with debug logging enabled.</li>
 *   <li><b>Reprocessing</b> – re-derive output files after a bug fix without manually
 *       re-running the full pipeline from scratch.</li>
 *   <li><b>Audit</b> – verify that a historic run produces the expected output.</li>
 * </ul>
 *
 * <h2>Limitations</h2>
 * <p>Replay re-executes with the current input <em>data</em> (the same file paths), not
 * necessarily the same data. If source files have changed since the original run, the replay
 * will produce different output. True data replay would require snapshotting input files,
 * which is outside the scope of this implementation.
 */
public class ReplayService {
    private static final Logger logger = Logger.getLogger(ReplayService.class.getName());

    /**
     * Replays the pipeline run identified by the given run ID.
     *
     * <p>Steps:
     * <ol>
     *   <li>Retrieve the {@link PipelineRun} from the {@link JsonPipelineRunManager}.</li>
     *   <li>Extract the XML snapshot string.</li>
     *   <li>Write it to a temp file at {@code /tmp/replay_<UUID>.xml}.</li>
     *   <li>Call {@link org.example.datapipeline.cli.Pipeline#run(String)} with that temp
     *       file path.</li>
     *   <li>Delete the temp file in a {@code finally} block.</li>
     * </ol>
     *
     * @param runId the UUID string of the run to replay (must exist in the {@code runs/} dir)
     * @throws Exception if the run is not found, the snapshot is empty, or the pipeline fails
     */
    public static void replay(String runId) throws Exception {
        logger.info("REPLAY_START run_id=" + runId);
        
        PipelineRunManager manager = new JsonPipelineRunManager();
        PipelineRun oldRun = manager.getRun(runId);
        
        String xmlSnapshot = oldRun.getXmlSnapshot();
        if (xmlSnapshot == null || xmlSnapshot.isEmpty()) {
            throw new RuntimeException("No XML snapshot found for run " + runId);
        }

        // Write snapshot to a temporary file for parsing
        String tempXmlPath = "/tmp/replay_" + UUID.randomUUID() + ".xml";
        Files.writeString(Paths.get(tempXmlPath), xmlSnapshot, java.nio.charset.StandardCharsets.UTF_8);
        
        try {
            // Rerun the pipeline using the exact snapshot
            Pipeline.run(tempXmlPath);
        } finally {
            new File(tempXmlPath).delete();
            logger.info("REPLAY_END run_id=" + runId);
        }
    }
}
