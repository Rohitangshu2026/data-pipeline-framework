package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;

import java.util.*;

/**
 * Represents a stage in the pipeline workflow.
 *
 * A stage is a logical unit of execution within a pipeline job.
 * Each stage contains one or more tasks that are executed sequentially.
 * Stages may declare dependencies on other stages, which determines
 * the execution order of the pipeline.
 *
 * The dependencies are defined in the XML configuration using the
 * "pre_req" attribute and are normalized into a set of stage identifiers
 * for easier processing by downstream components such as the DAG builder.
 *
 * This class is mapped from the <stage> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Stage {

    @XmlAttribute
    private String id;

    @XmlAttribute(name = "pre_req")
    private String preReq;

    @XmlElement(name = "task")
    private List<Task> tasks = new ArrayList<>();

    @XmlElement(name = "on_error")
    private OnError onError;

    private Set<String> dependencies = new LinkedHashSet<>();

    /**
     * Returns the identifier of the stage.
     *
     * @return stage id defined in the pipeline configuration
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the set of stage dependencies.
     *
     * @return set of stage ids that this stage depends on
     */
    public Set<String> getDependencies() {
        return dependencies;
    }

    /**
     * Returns the list of tasks contained in this stage.
     *
     * @return list of tasks executed within the stage
     */
    public List<Task> getTasks() {
        return tasks;
    }

    /**
     * Returns the error handling configuration for this stage.
     *
     * @return OnError configuration if defined
     */
    public OnError getOnError() {
        return onError;
    }

    /**
     * Converts the "pre_req" attribute from the XML configuration
     * into a normalized set of stage dependencies.
     *
     * The dependency string is split on whitespace and stored in
     * the dependencies set. Self-dependencies are rejected.
     */
    public void normalizeDependencies() {

        if(preReq == null || preReq.trim().isEmpty())
            return;

        String[] deps = preReq.trim().split("\\s+");

        for(String dep : deps) {

            if(dep.equals(id))
                throw new RuntimeException("Stage cannot depend on itself: " + id);

            dependencies.add(dep);
        }
    }
}