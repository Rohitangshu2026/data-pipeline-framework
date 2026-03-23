package org.example.datapipeline.executor.metrics;

public class TaskMetrics {
    private long startTime;
    private long endTime;
    private long rowsIn;
    private long rowsOut;
    private boolean success;
    private String error;

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void end() {
        endTime = System.currentTimeMillis();
    }

    public long getDuration() {
        return endTime - startTime;
    }

    public void setRowsIn(long rowsIn) {
        this.rowsIn = rowsIn;
    }

    public void setRowsOut(long rowsOut) {
        this.rowsOut = rowsOut;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setError(String error){
        this.error = error;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getError() {
        return error;
    }

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