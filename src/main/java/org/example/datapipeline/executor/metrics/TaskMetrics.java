package org.example.datapipeline.executor.metrics;

/**
 * Captures execution metrics for a single pipeline task.
 *
 * <p>{@code TaskMetrics} is a simple value object populated by
 * {@link org.example.datapipeline.executor.PipelineExecutor#executeStage} during task
 * execution. It records wall-clock timing (via {@link #start()} and {@link #end()}), row
 * throughput, and success/failure status. The populated metrics are logged at INFO level
 * after each task completes and are also stored in the
 * {@link org.example.datapipeline.versioning.TaskRun} for the persistent run record.
 *
 * <p>Instance lifecycle:
 * <ol>
 *   <li>Create a new instance per task.</li>
 *   <li>Call {@link #start()} before the action executor runs.</li>
 *   <li>Set {@link #setRowsIn(long)} and {@link #setRowsOut(long)} after the action and
 *       writer complete.</li>
 *   <li>Call {@link #setSuccess(boolean)} and optionally {@link #setError(String)}.</li>
 *   <li>Call {@link #end()} to record the end timestamp.</li>
 *   <li>Read {@link #getDuration()} for elapsed milliseconds.</li>
 * </ol>
 */
public class TaskMetrics {
    private long startTime;
    private long endTime;
    private long rowsIn;
    private long rowsOut;
    private boolean success;
    private String error;

    /** Records the task start time as the current wall-clock time in milliseconds. */
    public void start() {
        startTime = System.currentTimeMillis();
    }

    /** Records the task end time as the current wall-clock time in milliseconds. */
    public void end() {
        endTime = System.currentTimeMillis();
    }

    /**
     * Returns the wall-clock duration of the task in milliseconds.
     *
     * <p>Only meaningful after both {@link #start()} and {@link #end()} have been called.
     *
     * @return elapsed milliseconds between {@code start()} and {@code end()}
     */
    public long getDuration() {
        return endTime - startTime;
    }

    /**
     * Sets the number of rows consumed from the input iterator (including the header row).
     *
     * @param rowsIn row count from the input {@link org.example.datapipeline.executor.metrics.CountingIterator}
     */
    public void setRowsIn(long rowsIn) {
        this.rowsIn = rowsIn;
    }

    /**
     * Sets the number of rows produced by the output iterator (including the header row).
     *
     * @param rowsOut row count from the output {@link org.example.datapipeline.executor.metrics.CountingIterator}
     */
    public void setRowsOut(long rowsOut) {
        this.rowsOut = rowsOut;
    }

    /**
     * Records whether the task completed without throwing an exception.
     *
     * @param success {@code true} if the task succeeded; {@code false} if it threw
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /**
     * Records the error message when the task fails.
     *
     * @param error the exception message, or {@code null} on success
     */
    public void setError(String error){
        this.error = error;
    }

    /** @return {@code true} if the task completed without throwing an exception */
    public boolean isSuccess() {
        return success;
    }

    /** @return the error message set by {@link #setError}, or {@code null} on success */
    public String getError() {
        return error;
    }

    /**
     * Returns a human-readable summary of the task metrics.
     *
     * @return formatted string with duration, row counts, success flag, and optional error
     */
    @Override
    public String toString() {
        return String.format(
                "Time=%dms, RowsIn=%d, RowsOut=%d, Success=%s%s",
                getDuration(),
                rowsIn,
                rowsOut,
                success,
                error != null ? ", Error=" + error : ""
        );
    }
}