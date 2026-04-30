package org.example.datapipeline.config.action;

import jakarta.xml.bind.annotation.*;

/**
 * Immutable key-value pair carrying a single configuration parameter.
 *
 * <p>{@code Param} is the leaf element of the pipeline configuration hierarchy.
 * It appears in two contexts:
 * <ul>
 *   <li>Under {@code <method>} inside a {@code <task>}, where it supplies runtime
 *       arguments to an action executor (e.g. the column name to filter, the sort
 *       order, the path to a bash script).</li>
 *   <li>Under {@code <datasource>}, where it carries connection parameters such as
 *       the file path ({@code name="src"}), API URL, or JSON path.</li>
 * </ul>
 *
 * <p>Parameters are surfaced as a flat {@code Map<String, String>} by
 * {@link Method#getParamMap()} and by the datasource resolution logic in
 * {@link org.example.datapipeline.config.input.Input#resolve} and
 * {@link org.example.datapipeline.config.output.Output#resolve}.
 *
 * <p>Typical XML representation:
 * <pre>{@code
 * <param name="column" value="price"/>
 * }</pre>
 *
 * <p>This class is mapped from the {@code <param>} element in the pipeline XML by JAXB.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Param {

    @XmlAttribute
    private String name;

    @XmlAttribute
    private String value;

    /**
     * Returns the parameter name (the key used to look up this value).
     *
     * @return parameter name string; never {@code null} for a valid XML document
     */
    public String getName(){
        return name;
    }

    /**
     * Returns the parameter value as a raw string.
     *
     * <p>Type conversion (to int, double, boolean, etc.) is the responsibility of the
     * consuming executor or strategy. The XML schema enforces that the value attribute
     * is present; it may however be an empty string.
     *
     * @return parameter value string; may be empty but never {@code null} for a valid config
     */
    public String getValue(){
        return value;
    }
}
