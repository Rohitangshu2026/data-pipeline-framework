package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;

import java.util.*;

/**
 * Represents the root configuration of a pipeline job.
 *
 * A Job defines the complete pipeline structure as specified
 * in the XML configuration file. It contains the list of stages
 * that make up the pipeline workflow.
 *
 * Each stage represents a unit of execution and may depend on
 * other stages through declared dependencies.
 *
 * This class is mapped from the <job> element in the pipeline XML.
 */
@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class Job {

    @XmlAttribute
    private String id;

    @XmlElement(name = "stage")
    private List<Stage> stages = new ArrayList<>();

    private Map<String, Stage> stageMap = new HashMap<>();

    /**
     * Returns the identifier of the pipeline job.
     *
     * @return job id defined in the pipeline configuration
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the list of stages defined in the pipeline.
     *
     * @return ordered list of pipeline stages
     */
    public List<Stage> getStages() {
        return stages;
    }

    /**
     * Returns a lookup map for stages indexed by stage id.
     *
     * @return map of stage id to stage object
     */
    public Map<String, Stage> getStageMap() {
        return stageMap;
    }

    /**
     * Builds a lookup map of stage ids to stage objects.
     * This allows efficient access to stages when resolving
     * dependencies during pipeline processing.
     */
    public void buildStageMap() {
        for(Stage s : stages) {
            stageMap.put(s.getId(), s);
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

        // Build graph
        for(Stage stage : stages) {

            for(String dep : stage.getDependencies()) {

                graph.get(dep).add(stage.getId());

                indegree.put(
                        stage.getId(),
                        indegree.get(stage.getId()) + 1
                );
            }
        }

        Queue<String> queue = new LinkedList<>();

        for(String id : indegree.keySet()) {
            if(indegree.get(id) == 0)
                queue.add(id);
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

                    if(indegree.get(next) == 0)
                        queue.add(next);
                }
            }

            levels.add(level);
        }

        // cycle detection
        if(processed != stages.size()) {
            throw new RuntimeException("Pipeline contains cyclic dependencies");
        }

        return levels;
    }
}