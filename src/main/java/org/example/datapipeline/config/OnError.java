package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;

/**
 * Represents the error handling configuration for a pipeline stage.
 *
 * This configuration defines how the framework should respond when
 * a task within a stage fails during execution. The strategy and
 * retry configuration are defined in the pipeline XML.
 *
 * This class is mapped from the <on_error> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class OnError {

    @XmlAttribute(name = "handling_strategy")
    private String handlingStrategy;

    @XmlAttribute(name = "retry_count")
    private Integer retryCount;

    /**
     * Returns the failure handling strategy configured for the stage.
     *
     * @return error handling strategy (proceed, retry, abort)
     */
    public String getHandlingStrategy() {
        return handlingStrategy;
    }

    /**
     * Returns the number of retry attempts configured for the stage.
     *
     * @return retry count if retries are enabled
     */
    public Integer getRetryCount() {
        return retryCount;
    }
}