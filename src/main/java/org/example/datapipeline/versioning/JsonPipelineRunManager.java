package org.example.datapipeline.versioning;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JsonPipelineRunManager implements PipelineRunManager {
    private final String storageDir = "runs";

    public JsonPipelineRunManager() {
        new File(storageDir).mkdirs();
    }

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
