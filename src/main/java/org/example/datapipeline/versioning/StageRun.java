package org.example.datapipeline.versioning;

import java.util.ArrayList;
import java.util.List;

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
