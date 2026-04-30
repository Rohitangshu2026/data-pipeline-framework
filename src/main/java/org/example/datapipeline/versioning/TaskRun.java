package org.example.datapipeline.versioning;

/**
 * Value object recording the execution details of a single pipeline task.
 *
 * <p>A {@code TaskRun} is created per task within
 * {@link org.example.datapipeline.executor.PipelineExecutor#executeStage} and populated
 * with row counts, duration, and success/failure status after the task completes. It is
 * appended to the parent {@link StageRun}.
 *
 * <p>Fields captured:
 * <ul>
 *   <li>{@code taskId}   – the action type string of the task (e.g. {@code "transform"},
 *                          {@code "join"}, {@code "bash"})</li>
 *   <li>{@code rowsIn}   – number of rows consumed from the input iterator (inc. header)</li>
 *   <li>{@code rowsOut}  – number of rows produced by the output iterator (inc. header)</li>
 *   <li>{@code duration} – wall-clock milliseconds from task start to end</li>
 *   <li>{@code success}  – {@code true} if no exception was thrown</li>
 *   <li>{@code error}    – the exception message if {@code success} is {@code false}</li>
 * </ul>
 *
 * <p>Together with {@link StageRun} and {@link PipelineRun}, task runs provide a full
 * audit trail: which tasks processed how many rows, how long each took, and whether any
 * failed.
 */
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
