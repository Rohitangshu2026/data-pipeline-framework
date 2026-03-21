package org.example.datapipeline.config.action;

import jakarta.xml.bind.annotation.*;

/**
 * Represents a key-value parameter for an action method.
 *
 * Each parameter is defined as an attribute pair (name, value)
 * and is used to configure the behavior of a specific action method
 * at runtime.
 *
 * This class is mapped from a <param> element in the pipeline XML,
 * allowing dynamic and flexible parameterization of actions without
 * hardcoding values in the code.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Param {

    @XmlAttribute
    private String name;

    @XmlAttribute
    private String value;

    public String getName(){
        return name;
    }

    public String getValue(){
        return value;
    }
}
