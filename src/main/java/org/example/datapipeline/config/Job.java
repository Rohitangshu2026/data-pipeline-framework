package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;
import java.util.*;

/**
 * Root configuration object representing a complete pipeline job.
 *
 * <p>A {@code Job} is the top-level element in the pipeline XML ({@code <job id="...">}).
 * It contains two main sections:
 * <ul>
 *   <li><b>Datasources</b> – a named catalogue of reusable I/O endpoints. Tasks can
 *       reference these by {@code ref} attribute to avoid repeating file paths or
 *       connection parameters throughout the XML.</li>
 *   <li><b>Stages</b> – the ordered (or partially ordered) list of processing stages.
 *       Dependency relationships between stages are expressed via the {@code pre_req}
 *       attribute and are resolved into a DAG at runtime.</li>
 * </ul>
 *
 * <p>After JAXB deserialisation, the caller must invoke {@link #resolveDatasources()} to
 * inline datasource parameters into each task's input/output descriptors, and then
 * {@link #buildStageMap()} / {@link #getExecutionLevels()} to construct the DAG.
 *
 * <p>This class is mapped from the {@code <job>} root element by JAXB and validated
 * against the bundled {@code job.xsd} schema during parsing.
 */
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

    /** @return the pipeline job identifier declared in the {@code id} XML attribute */
    public String getId() { return id; }

    /** @return the ordered list of global datasource definitions */
    public List<Datasource> getDatasources() { return datasources; }

    /** @return the ordered list of pipeline stages as declared in the XML */
    public List<Stage> getStages() { return stages; }

    /**
     * Returns the stage lookup map, keyed by stage ID.
     *
     * <p>This map is populated by {@link #buildStageMap()} and is used internally
     * by the topological sort. It must not be mutated externally.
     *
     * @return mutable map of stage-id → Stage
     */
    public Map<String, Stage> getStageMap() { return stageMap; }

    /**
     * Returns the datasource lookup map, keyed by datasource ID.
     *
     * <p>This map is populated by {@link #buildStageMap()} and is used by
     * {@link #resolveDatasources()} to inline datasource parameters into task I/O.
     *
     * @return mutable map of datasource-id → Datasource
     */
    public Map<String, Datasource> getDatasourceMap() { return datasourceMap; }

    /**
     * Builds the in-memory lookup maps for stages and datasources from the parsed lists.
     *
     * <p>This method is idempotent and safe to call multiple times. It must be called
     * before {@link #getExecutionLevels()} and before any code that resolves datasource
     * references. {@link org.example.datapipeline.util.ConfigNormalizer} calls this as
     * part of the normalisation phase.
     */
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

    /**
     * Inlines global datasource parameters into each task's input and output descriptors.
     *
     * <p>When a task declares {@code <input ref="ds_id"/>}, this method looks up the
     * corresponding {@link Datasource} in the global catalogue and copies its parameters
     * (type, src, etc.) into the task's {@link org.example.datapipeline.config.input.Input}
     * or {@link org.example.datapipeline.config.output.Output} object. After this call,
     * task I/O objects are fully self-contained and no longer need the global map at
     * runtime (though the map remains available for join actions that reference datasources
     * dynamically via the execution context metadata).
     *
     * <p>This method also calls {@link #buildStageMap()} to ensure lookup maps are ready.
     */
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

    /**
     * Computes the parallel execution levels of the pipeline using Kahn's BFS algorithm.
     *
     * <p>The algorithm works as follows:
     * <ol>
     *   <li>Build a directed graph where an edge {@code A → B} means "B depends on A".</li>
     *   <li>Compute the in-degree (number of unresolved dependencies) for each stage.</li>
     *   <li>Seed a queue with all stages whose in-degree is 0 (no dependencies).</li>
     *   <li>While the queue is non-empty, drain the entire current queue into one
     *       <em>level</em> (these stages can all run in parallel), then decrement the
     *       in-degree of their successors and enqueue any that reach 0.</li>
     * </ol>
     *
     * <p>If not all stages are processed when the queue empties (i.e., processed count
     * &lt; stages.size()), a cycle exists in the dependency graph and a
     * {@link RuntimeException} is thrown.
     *
     * <p>The returned list is ordered: level 0 runs first, level 1 second, and so on.
     * All stages within the same level have no dependencies on each other and are
     * executed concurrently via {@code parallelStream} in
     * {@link org.example.datapipeline.executor.PipelineExecutor}.
     *
     * @return an ordered list of execution levels, each level being a list of stages
     *         that can run in parallel
     * @throws RuntimeException if the stage dependency graph contains a cycle
     */
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