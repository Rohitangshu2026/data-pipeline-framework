package org.example.datapipeline.config.action;

import jakarta.xml.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Represents a method configuration for an action in the pipeline.
 *
 * A method defines the specific operation to be executed along with
 * its associated parameters. It is configured in the pipeline XML
 * and linked to an action.
 *
 * Provides access to the method name and a mapping of
 * parameter names to their corresponding values for easier use
 * during execution.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Method {

    @XmlAttribute
    private String name;

    @XmlElement(name = "param")
    private List<Param> params;

    public String getName(){
        return name;
    }

    public Map<String, String> getParamMap(){
        Map<String, String> map = new HashMap<>();
        if (params != null) {
            for (Param p : params) {
                map.put(p.getName(), p.getValue());
            }
        }
        return map;
    }
}