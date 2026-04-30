package org.example.datapipeline.versioning;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable-by-convention value object representing a single pipeline execution record.
 *
 * <p>A {@code PipelineRun} is created at the start of every
 * {@link org.example.datapipeline.executor.PipelineExecutor#execute} call, updated as
 * stages complete, and persisted by {@link PipelineRunManager} at the end of execution
 * (regardless of success or failure).
 *
 * <p>Fields captured:
 * <ul>
 *   <li>{@code runId}       – UUID string uniquely identifying this execution</li>
 *   <li>{@code xmlSnapshot} – the raw XML content of the pipeline configuration at the
 *                             time of execution (enabling exact replay)</li>
 *   <li>{@code startTime}   – epoch millisecond when the execution began</li>
 *   <li>{@code endTime}     – epoch millisecond when the execution finished</li>
 *   <li>{@code status}      – {@code "RUNNING"}, {@code "SUCCESS"}, or {@code "FAILED"}</li>
 *   <li>{@code stages}      – ordered list of {@link StageRun} objects (one per stage)</li>
 * </ul>
 *
 * <p>The {@link #addStage(StageRun)} method is called from within a {@code synchronized}
 * block in the executor because multiple stage threads may attempt to add concurrently.
 */
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
