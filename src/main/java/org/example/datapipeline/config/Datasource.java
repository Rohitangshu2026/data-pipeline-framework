package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;
import org.example.datapipeline.config.action.Param;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a named, reusable I/O endpoint declared in the {@code <datasources>} block.
 *
 * <p>Datasources centralise connection parameters so that multiple tasks can reference the
 * same file path, API URL, or other resource without repeating the configuration. A task
 * references a datasource by setting the {@code ref} attribute on its {@code <input>} or
 * {@code <output>} element to the datasource's {@code id}.
 *
 * <p>During {@link org.example.datapipeline.config.Job#resolveDatasources()}, the framework
 * copies the datasource's type and parameter list into the task's
 * {@link org.example.datapipeline.config.input.Input} or
 * {@link org.example.datapipeline.config.output.Output} descriptor. After resolution, the
 * datasource object is no longer directly accessed by the execution engine (except for join
 * actions that resolve {@code right_ref} dynamically from the execution-context metadata).
 *
 * <p>Typical XML representation:
 * <pre>{@code
 * <datasource id="ds_purchases" type="csv">
 *   <param name="src" value="target/purchases.csv"/>
 * </datasource>
 * }</pre>
 *
 * <p>This class is mapped from the {@code <datasource>} element in the pipeline XML by JAXB.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Datasource {
    @XmlAttribute
    private String id;

    @XmlAttribute
    private String type;

    @XmlElement(name = "param")
    private List<Param> params = new ArrayList<>();

    /** @return the unique datasource identifier referenced by task {@code ref} attributes */
    public String getId() { return id; }

    /**
     * Returns the I/O type for this datasource (e.g. {@code "csv"}, {@code "api"}).
     *
     * <p>This value is used by {@link org.example.datapipeline.executor.io.DataIORegistry}
     * to select the appropriate {@link org.example.datapipeline.executor.io.DataReader} or
     * {@link org.example.datapipeline.executor.io.DataWriter} implementation.
     *
     * @return lowercase type string
     */
    public String getType() { return type; }

    /** @return the ordered list of key-value parameters for this datasource */
    public List<Param> getParams() { return params; }
}
