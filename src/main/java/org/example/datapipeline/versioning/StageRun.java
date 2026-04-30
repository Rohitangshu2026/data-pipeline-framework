package org.example.datapipeline.versioning;

import java.util.ArrayList;
import java.util.List;

/**
 * Value object recording the execution details of a single pipeline stage.
 *
 * <p>A {@code StageRun} is created at the start of
 * {@link org.example.datapipeline.executor.PipelineExecutor#executeStage} and populated
 * as the stage executes. It is appended to the parent {@link PipelineRun} immediately
 * after creation (inside a {@code synchronized} block to prevent concurrent-modification
 * races). After execution completes, the status is updated to {@code "SUCCESS"} or
 * {@code "FAILED"} and the end time is set.
 *
 * <p>Fields captured:
 * <ul>
 *   <li>{@code stageId}   – the stage identifier from the XML configuration</li>
 *   <li>{@code startTime} – epoch millisecond when stage execution began</li>
 *   <li>{@code endTime}   – epoch millisecond when stage execution finished</li>
 *   <li>{@code status}    – {@code "RUNNING"}, {@code "SUCCESS"}, or {@code "FAILED"}</li>
 *   <li>{@code tasks}     – ordered list of {@link TaskRun} objects (one per task)</li>
 * </ul>
 */
public class StageRun {
    private String stageId;
    private long startTime;
    private long endTime;
    private String status;
    private List<TaskRun> tasks = new ArrayList<>();

    public StageRun(String stageId) { this.stageId = stageId; }

    public String getStageId() { return stageId; }
    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public List<TaskRun> getTasks() { return tasks; }
    public void addTask(TaskRun task) { this.tasks.add(task); }
}
