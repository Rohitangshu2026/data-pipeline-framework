package org.example.datapipeline.versioning;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON-file-backed implementation of {@link PipelineRunManager}.
 *
 * <p>Persists each {@link PipelineRun} as a pretty-printed JSON file in the {@code runs/}
 * directory (relative to the process working directory). The file is named
 * {@code <runId>.json}. The directory is created on construction if it does not exist.
 *
 * <h2>JSON Schema</h2>
 * <pre>{@code
 * {
 *   "runId": "...",
 *   "xmlSnapshot": "<job id=...>...</job>",
 *   "startTime": 1700000000000,
 *   "endTime":   1700000060000,
 *   "status": "SUCCESS",
 *   "stages": [
 *     {
 *       "stageId": "filter_purchases",
 *       "startTime": ..., "endTime": ..., "status": "SUCCESS",
 *       "tasks": [
 *         { "taskId": "transform", "rowsIn": 67500000, "rowsOut": 916940,
 *           "duration": 123456, "success": true }
 *       ]
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>Uses the {@code org.json} library for serialisation and deserialisation.
 * Serialisation errors are printed to {@code System.err} but do not throw, so a failed
 * save does not interrupt the pipeline. Deserialisation errors (in {@link #getRun}) do
 * throw because the caller explicitly requested a specific run.
 */
public class JsonPipelineRunManager implements PipelineRunManager {
    private final String storageDir = "runs";

    /**
     * Creates the manager and ensures the {@code runs/} storage directory exists.
     */
    public JsonPipelineRunManager() {
        new File(storageDir).mkdirs();
    }

    /**
     * Serialises the given run to a JSON file at {@code runs/<runId>.json}.
     *
     * <p>The full {@link PipelineRun} object graph (including all stage and task runs) is
     * serialised. If writing fails, the error is printed to {@code System.err} and the
     * method returns normally (non-fatal).
     *
     * @param run the pipeline run to persist; must not be {@code null}
     */
    @Override
    public void saveRun(PipelineRun run) {
        try {
            JSONObject runJson = new JSONObject();
            runJson.put("runId", run.getRunId());
            runJson.put("xmlSnapshot", run.getXmlSnapshot());
            runJson.put("startTime", run.getStartTime());
            runJson.put("endTime", run.getEndTime());
            runJson.put("status", run.getStatus());

            JSONArray stagesArray = new JSONArray();
            for (StageRun stage : run.getStages()) {
                JSONObject stageJson = new JSONObject();
                stageJson.put("stageId", stage.getStageId());
                stageJson.put("startTime", stage.getStartTime());
                stageJson.put("endTime", stage.getEndTime());
                stageJson.put("status", stage.getStatus());

                JSONArray tasksArray = new JSONArray();
                for (TaskRun task : stage.getTasks()) {
                    JSONObject taskJson = new JSONObject();
                    taskJson.put("taskId", task.getTaskId());
                    taskJson.put("rowsIn", task.getRowsIn());
                    taskJson.put("rowsOut", task.getRowsOut());
                    taskJson.put("duration", task.getDuration());
                    taskJson.put("success", task.isSuccess());
                    if (task.getError() != null) taskJson.put("error", task.getError());
                    tasksArray.put(taskJson);
                }
                stageJson.put("tasks", tasksArray);
                stagesArray.put(stageJson);
            }
            runJson.put("stages", stagesArray);

            Files.writeString(Paths.get(storageDir, run.getRunId() + ".json"), runJson.toString(2), StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.err.println("Failed to save run: " + e.getMessage());
        }
    }

    /**
     * Deserialises and returns the pipeline run stored at {@code runs/<runId>.json}.
     *
     * <p>Reconstructs the full {@link PipelineRun} object graph including all
     * {@link StageRun} and {@link TaskRun} children.
     *
     * @param runId the UUID string of the run to retrieve
     * @return the deserialised {@link PipelineRun}
     * @throws RuntimeException if the file does not exist or cannot be parsed
     */
    @Override
    public PipelineRun getRun(String runId) {
        try {
            String content = Files.readString(Paths.get(storageDir, runId + ".json"), StandardCharsets.UTF_8);
            JSONObject runJson = new JSONObject(content);
            PipelineRun run = new PipelineRun(runJson.getString("runId"));
            run.setXmlSnapshot(runJson.getString("xmlSnapshot"));
            run.setStartTime(runJson.getLong("startTime"));
            run.setEndTime(runJson.getLong("endTime"));
            run.setStatus(runJson.getString("status"));
            
            if (runJson.has("stages")) {
                JSONArray stagesArray = runJson.getJSONArray("stages");
                for (int i = 0; i < stagesArray.length(); i++) {
                    JSONObject stageJson = stagesArray.getJSONObject(i);
                    StageRun stageRun = new StageRun(stageJson.getString("stageId"));
                    stageRun.setStartTime(stageJson.getLong("startTime"));
                    stageRun.setEndTime(stageJson.getLong("endTime"));
                    stageRun.setStatus(stageJson.getString("status"));
                    
                    if (stageJson.has("tasks")) {
                        JSONArray tasksArray = stageJson.getJSONArray("tasks");
                        for (int j = 0; j < tasksArray.length(); j++) {
                            JSONObject taskJson = tasksArray.getJSONObject(j);
                            TaskRun taskRun = new TaskRun(taskJson.getString("taskId"));
                            taskRun.setRowsIn(taskJson.getLong("rowsIn"));
                            taskRun.setRowsOut(taskJson.getLong("rowsOut"));
                            taskRun.setDuration(taskJson.getLong("duration"));
                            taskRun.setSuccess(taskJson.getBoolean("success"));
                            if (taskJson.has("error")) {
                                taskRun.setError(taskJson.getString("error"));
                            }
                            stageRun.addTask(taskRun);
                        }
                    }
                    run.addStage(stageRun);
                }
            }
            return run;
        } catch (Exception e) {
            throw new RuntimeException("Run ID not found or could not be read: " + runId, e);
        }
    }

    /**
     * Lists the IDs of all runs saved in the {@code runs/} directory.
     *
     * <p>Returns the base filenames (without the {@code .json} extension) of all
     * {@code .json} files found in the storage directory. Order is filesystem-dependent.
     *
     * @return list of run ID strings; empty if no runs have been saved or directory is missing
     */
    @Override
    public List<String> listRuns() {
        List<String> runs = new ArrayList<>();
        File dir = new File(storageDir);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
            if (files != null) {
                for (File file : files) {
                    runs.add(file.getName().replace(".json", ""));
                }
            }
        }
        return runs;
    }
}
