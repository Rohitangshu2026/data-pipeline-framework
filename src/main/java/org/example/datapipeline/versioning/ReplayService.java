package org.example.datapipeline.versioning;

import org.example.datapipeline.cli.Pipeline;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.UUID;

public class ReplayService {
    private static final Logger logger = Logger.getLogger(ReplayService.class.getName());

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
