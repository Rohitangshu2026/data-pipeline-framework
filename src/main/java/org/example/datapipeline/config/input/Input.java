package org.example.datapipeline.config.input;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import org.example.datapipeline.config.action.Param;
import org.example.datapipeline.config.Datasource;
import org.example.datapipeline.executor.io.DataIORegistry;

import jakarta.xml.bind.annotation.*;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration object representing the {@code <input>} element inside a {@code <task>}.
 *
 * <p>An {@code Input} describes where a task reads its data from. It supports two
 * resolution modes:
 * <ul>
 *   <li><b>Inline</b> – the type and parameters are defined directly on the element:
 *       {@code <input type="csv"><param name="src" value="path/to/file.csv"/></input>}</li>
 *   <li><b>Referenced</b> – a {@code ref} attribute points to a global datasource ID:
 *       {@code <input ref="ds_purchases"/>}. The datasource's type and parameters are
 *       merged in during {@link #resolve}.</li>
 * </ul>
 *
 * <p>After {@link #resolve} is called, the {@code Input} object is fully self-contained:
 * {@link #getSrc()} returns the file path (or equivalent), and {@link #streamData()} opens
 * a streaming {@link org.example.datapipeline.executor.iterator.DataIterator} backed by the
 * appropriate reader (CSV, API, etc.) looked up from
 * {@link org.example.datapipeline.executor.io.DataIORegistry}.
 *
 * <p>Inline params defined directly on the {@code <input>} element always override params
 * inherited from the referenced datasource.
 *
 * <p>This class is mapped from the {@code <input>} element in the pipeline XML by JAXB.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Input {

    @XmlAttribute
    private String type;

    @XmlAttribute
    private String ref;

    @XmlElement(name = "param")
    private List<Param> params = new ArrayList<>();

    private transient Map<String, String> resolvedParams = new HashMap<>();

    /**
     * Resolves this input against the global datasource catalogue.
     *
     * <p>If a {@code ref} attribute is set and the referenced datasource exists in
     * {@code globals}, this method copies the datasource's type (if not already set
     * inline) and all its parameters into the resolved parameter map. Inline {@code <param>}
     * elements declared directly on this {@code <input>} are merged afterwards, so they
     * take precedence over any inherited datasource parameters.
     *
     * <p>This method is called by
     * {@link org.example.datapipeline.config.Job#resolveDatasources()} during the
     * pre-execution normalisation phase.
     *
     * @param globals the job-level datasource lookup map, keyed by datasource ID
     */
    public void resolve(Map<String, Datasource> globals) {
        if (ref != null && globals.containsKey(ref)) {
            Datasource ds = globals.get(ref);
            if (this.type == null) this.type = ds.getType();
            for (Param p : ds.getParams()) resolvedParams.put(p.getName(), p.getValue());
        }
        for (Param p : params) resolvedParams.put(p.getName(), p.getValue());
    }

    /** @return the I/O type string (e.g. {@code "csv"}, {@code "api"}) used to select the reader */
    public String getType() { return type; }

    /**
     * Returns the resolved value of a named parameter.
     *
     * @param name parameter key (e.g. {@code "src"}, {@code "url"})
     * @return parameter value or {@code null} if not defined
     */
    public String getParam(String name) { return resolvedParams.get(name); }

    /**
     * Convenience accessor for the {@code src} parameter, which holds the file path
     * for CSV-type inputs.
     *
     * @return the value of the {@code src} parameter, or {@code null} if not set
     */
    public String getSrc() {
        return getParam("src");
    }

    /**
     * Opens a streaming iterator over this input's data.
     *
     * <p>Delegates to {@link org.example.datapipeline.executor.io.DataIORegistry} to
     * select the correct {@link org.example.datapipeline.executor.io.DataReader}
     * implementation based on {@link #getType()}, then calls
     * {@link org.example.datapipeline.executor.io.DataReader#createIterator} with the
     * resolved parameter map. The returned iterator is lazy – no data is read until
     * {@link DataIterator#next()} is called.
     *
     * @return a new {@link DataIterator} positioned before the header row
     * @throws RuntimeException if the type is unsupported or the source cannot be opened
     */
    public DataIterator streamData() {
        return DataIORegistry.getReader(this.type).createIterator(resolvedParams);
    }
}