package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;

/**
 * Represents the output destination produced by a task in a pipeline stage.
 *
 * The output defines the dataset or resource generated after the task
 * completes execution. This value is specified in the pipeline XML
 * configuration and can be used as the input for subsequent tasks or stages.
 *
 * This class is mapped from the <output> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Output {

    @XmlAttribute
    private String src;

    /**
     * Returns the output destination identifier.
     *
     * @return output source defined in the pipeline configuration
     */
    public String getSrc() {
        return src;
    }
}