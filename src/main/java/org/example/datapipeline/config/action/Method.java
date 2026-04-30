package org.example.datapipeline.config.action;

import jakarta.xml.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Configuration object representing the {@code <method>} element inside an {@code <action>}.
 *
 * <p>A method binds a named operation (e.g. {@code "filter"}, {@code "aggregate"}) to its
 * runtime parameters. The operation name is used by the executor to select the appropriate
 * strategy or handler, and the parameters supply the values needed to execute that operation
 * (e.g. which column to filter, what operator and value to apply).
 *
 * <p>Typical XML representation:
 * <pre>{@code
 * <method name="aggregate">
 *   <param name="group_by"   value="brand"/>
 *   <param name="column"     value="price"/>
 *   <param name="operation"  value="sum"/>
 * </method>
 * }</pre>
 *
 * <p>This class is mapped from the {@code <method>} element in the pipeline XML by JAXB.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Method {

    @XmlAttribute
    private String name;

    @XmlElement(name = "param")
    private List<Param> params;

    /**
     * Returns the operation name for this method.
     *
     * <p>Action executors use this value (lowercased) to dispatch to the correct
     * strategy or handler. For example, {@link org.example.datapipeline.executor.action.transform.TransformAction}
     * maps it to a registered {@link org.example.datapipeline.executor.action.transform.TransformStrategy}.
     *
     * @return method name string (e.g. {@code "filter"}, {@code "sort"}, {@code "run"})
     */
    public String getName(){
        return name;
    }

    /**
     * Returns the method's parameters as a convenient name-to-value map.
     *
     * <p>This map is constructed fresh on each call by iterating the underlying
     * {@link Param} list. Callers should obtain the map once and cache it locally
     * to avoid repeated list traversals.
     *
     * <p>If the same parameter name appears more than once in the XML, the last
     * occurrence wins (standard {@link java.util.HashMap} put semantics).
     *
     * @return mutable map of parameter names to their string values;
     *         returns an empty map if no parameters are defined
     */
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