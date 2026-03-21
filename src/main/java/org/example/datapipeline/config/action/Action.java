package org.example.datapipeline.config.action;

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

    @XmlElement
    private Method method;

    public String getType(){
        return type;
    }
    public Method getMethod(){
        return method;
    }
}