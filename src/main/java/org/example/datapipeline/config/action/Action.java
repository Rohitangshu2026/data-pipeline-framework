package org.example.datapipeline.config.action;

import jakarta.xml.bind.annotation.*;

/**
 * Configuration object representing the {@code <action>} element inside a {@code <task>}.
 *
 * <p>An action has two parts:
 * <ul>
 *   <li><b>type</b> – selects the {@link org.example.datapipeline.executor.action.ActionExecutor}
 *       implementation from the registry (e.g. {@code "transform"}, {@code "join"},
 *       {@code "bash"}, or any registered plugin type).</li>
 *   <li><b>method</b> – provides the operation name and its key-value parameters to the
 *       executor (e.g. {@code name="filter"}, {@code name="aggregate"}).</li>
 * </ul>
 *
 * <p>Typical XML representation:
 * <pre>{@code
 * <action type="transform">
 *   <method name="filter">
 *     <param name="column" value="event_type"/>
 *     <param name="operator" value="="/>
 *     <param name="value" value="purchase"/>
 *   </method>
 * </action>
 * }</pre>
 *
 * <p>This class is mapped from the {@code <action>} element in the pipeline XML by JAXB.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Action {

    @XmlAttribute
    private String type;

    @XmlElement
    private Method method;

    /**
     * Returns the action type that identifies the executor to use.
     *
     * <p>The value is matched case-insensitively against registered
     * {@link org.example.datapipeline.executor.action.ActionExecutor#getType()} values.
     *
     * @return action type string (e.g. {@code "transform"}, {@code "join"}, {@code "bash"})
     */
    public String getType(){
        return type;
    }

    /**
     * Returns the method configuration block for this action.
     *
     * <p>The method provides the operation name (e.g. {@code "filter"}) and all
     * key-value parameters needed by the executor at runtime.
     *
     * @return the method descriptor; never {@code null} for a valid configuration
     */
    public Method getMethod(){
        return method;
    }
}