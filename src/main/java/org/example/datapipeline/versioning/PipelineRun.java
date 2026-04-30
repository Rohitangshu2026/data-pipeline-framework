package org.example.datapipeline.versioning;

import java.util.ArrayList;
import java.util.List;

public class PipelineRun {
    private String runId;
    private String xmlSnapshot;
    private long startTime;
    private long endTime;
    private String status;
    private List<StageRun> stages = new ArrayList<>();

    public PipelineRun(String runId) { this.runId = runId; }

    public String getRunId() { return runId; }
    public String getXmlSnapshot() { return xmlSnapshot; }
    public void setXmlSnapshot(String xmlSnapshot) { this.xmlSnapshot = xmlSnapshot; }
    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public List<StageRun> getStages() { return stages; }
    public void addStage(StageRun stage) { this.stages.add(stage); }
}
