package org.example.datapipeline.versioning;

public class TaskRun {
    private String taskId;
    private long rowsIn;
    private long rowsOut;
    private long duration;
    private boolean success;
    private String error;

    public TaskRun(String taskId) { this.taskId = taskId; }

    public String getTaskId() { return taskId; }
    public long getRowsIn() { return rowsIn; }
    public void setRowsIn(long rowsIn) { this.rowsIn = rowsIn; }
    public long getRowsOut() { return rowsOut; }
    public void setRowsOut(long rowsOut) { this.rowsOut = rowsOut; }
    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
