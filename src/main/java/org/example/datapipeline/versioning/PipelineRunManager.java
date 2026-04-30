package org.example.datapipeline.versioning;

import java.util.List;

public interface PipelineRunManager {
    void saveRun(PipelineRun run);
    PipelineRun getRun(String runId);
    List<String> listRuns();
}
