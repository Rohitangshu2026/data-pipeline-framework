package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;

/**
 * Represents the action to be performed by a task in a pipeline stage.
 *
 * The action defines the operation that will be executed during
 * task execution, such as reading data, transforming data, or
 * writing results to an output destination.
 *
 * This class is mapped from the <action> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Action {

    @XmlAttribute
    private String type;

    /**
     * Returns the type of action associated with the task.
     *
     * @return action type defined in the pipeline configuration
     */
    public String getType() {
        return type;
    }
}