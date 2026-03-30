package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;
import java.util.*;

@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class Job {

    @XmlAttribute
    private String id;

    @XmlElementWrapper(name = "datasources")
    @XmlElement(name = "datasource")
    private List<Datasource> datasources = new ArrayList<>();

    @XmlElement(name = "stage")
    private List<Stage> stages = new ArrayList<>();

    private transient Map<String, Stage> stageMap = new HashMap<>();
    private transient Map<String, Datasource> datasourceMap = new HashMap<>();

    public String getId() { return id; }
    public List<Datasource> getDatasources() { return datasources; }
    public List<Stage> getStages() { return stages; }
    public Map<String, Stage> getStageMap() { return stageMap; }
    public Map<String, Datasource> getDatasourceMap() { return datasourceMap; }

    public void buildStageMap() {
        if(datasources != null) {
            for(Datasource ds : datasources) {
                datasourceMap.put(ds.getId(), ds);
            }
        }
        if(stages != null) {
            for(Stage s : stages) {
                stageMap.put(s.getId(), s);
            }
        }
    }

    public void resolveDatasources() {
        buildStageMap();
        if(stages != null) {
            for(Stage stage : stages) {
                if(stage.getTasks() != null) {
                    for(Task task : stage.getTasks()) {
                        if(task.getInput() != null) task.getInput().resolve(datasourceMap);
                        if(task.getOutput() != null) task.getOutput().resolve(datasourceMap);
                    }
                }
            }
        }
    }

    public List<List<Stage>> getExecutionLevels() {
        Map<String, Stage> stageMap = new HashMap<>();
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> graph = new HashMap<>();

        for(Stage s : stages) {
            stageMap.put(s.getId(), s);
            indegree.put(s.getId(), 0);
            graph.put(s.getId(), new ArrayList<>());
        }

        for(Stage stage : stages) {
            for(String dep : stage.getDependencies()) {
                graph.get(dep).add(stage.getId());
                indegree.put(stage.getId(), indegree.get(stage.getId()) + 1);
            }
        }

        Queue<String> queue = new LinkedList<>();
        for(String id : indegree.keySet()) {
            if(indegree.get(id) == 0) queue.add(id);
        }

        List<List<Stage>> levels = new ArrayList<>();
        int processed = 0;

        while(!queue.isEmpty()) {
            int size = queue.size();
            List<Stage> level = new ArrayList<>();
            for(int i = 0; i < size; i++) {
                String curr = queue.poll();
                processed++;
                level.add(stageMap.get(curr));
                for(String next : graph.get(curr)) {
                    indegree.put(next, indegree.get(next) - 1);
                    if(indegree.get(next) == 0) queue.add(next);
                }
            }
            levels.add(level);
        }

        if(processed != stages.size()) {
            throw new RuntimeException("Pipeline contains cyclic dependencies");
        }
        return levels;
    }
}