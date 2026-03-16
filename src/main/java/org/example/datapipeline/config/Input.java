package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;

/**
 * Represents the input source for a task in a pipeline stage.
 *
 * The input defines the source dataset that the task
 * will consume during execution. This value is specified in the
 * pipeline XML configuration.
 *
 * This class is mapped from the <input> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Input {

    @XmlAttribute
    private String src;

    /**
     * Returns the source identifier for the task input.
     *
     * @return the input source defined in the pipeline configuration
     */
    public String getSrc() {
        return src;
    }
}