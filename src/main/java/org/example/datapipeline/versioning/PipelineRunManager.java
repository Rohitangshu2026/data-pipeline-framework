package org.example.datapipeline.versioning;

import java.util.List;

/**
 * Storage abstraction for persisting and retrieving pipeline run records.
 *
 * <p>Every execution of a pipeline creates a {@link PipelineRun} capturing the run ID,
 * XML snapshot, timing, and per-stage/task metrics. Implementations of this interface
 * handle the mechanics of serialising and deserialising those records to a durable store.
 *
 * <p>The only built-in implementation is {@link JsonPipelineRunManager}, which persists
 * runs as JSON files in the {@code runs/} directory. Alternative implementations could
 * write to a database, an object store, or a remote service.
 *
 * <p>Run records enable:
 * <ul>
 *   <li><b>Auditability</b> – what ran, when, and with which configuration.</li>
 *   <li><b>Replay</b> – {@link ReplayService} retrieves the XML snapshot and re-runs the
 *       exact same pipeline configuration.</li>
 *   <li><b>Diagnostics</b> – per-stage and per-task row counts and durations for
 *       performance analysis.</li>
 * </ul>
 */
public interface PipelineRunManager {

    /**
     * Persists a completed (or failed) pipeline run record.
     *
     * <p>This method is always called in the executor's {@code finally} block so that
     * partial runs (e.g. a pipeline that failed mid-way) are still recorded with
     * {@code status = "FAILED"}.
     *
     * @param run the run record to save; must not be {@code null}
     */
    void saveRun(PipelineRun run);

    /**
     * Retrieves a previously saved pipeline run record by its ID.
     *
     * @param runId the UUID string of the run to retrieve
     * @return the deserialised {@link PipelineRun}; never {@code null}
     * @throws RuntimeException if the run ID is not found or the record cannot be read
     */
    PipelineRun getRun(String runId);

    /**
     * Lists the IDs of all saved pipeline runs.
     *
     * <p>Ordering is implementation-defined (typically filesystem order for the JSON
     * implementation).
     *
     * @return list of run ID strings; empty if no runs have been saved
     */
    List<String> listRuns();
}
