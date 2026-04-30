package org.example.datapipeline.config.output;

import jakarta.xml.bind.annotation.*;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.config.action.Param;
import org.example.datapipeline.config.Datasource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.io.BufferedWriter;
import java.io.File;
import org.example.datapipeline.executor.io.DataIORegistry;

/**
 * Configuration object representing the {@code <output>} element inside a {@code <task>}.
 *
 * <p>An {@code Output} describes where a task writes its result rows. Like
 * {@link org.example.datapipeline.config.input.Input}, it supports both inline parameter
 * definition and reference to a global datasource via the {@code ref} attribute. Resolution
 * semantics are identical: datasource parameters are merged first, then inline params
 * override.
 *
 * <p>The primary method consumers will call at runtime is {@link #writeData(DataIterator)},
 * which drains the iterator produced by the task's action and writes every row to the
 * configured destination using the appropriate
 * {@link org.example.datapipeline.executor.io.DataWriter}.
 *
 * <p>For actions that manage their own output (e.g. bash scripts that write directly to a
 * file), the executor skips the {@link #writeData} call entirely based on
 * {@link org.example.datapipeline.executor.action.ActionExecutor#handlesOwnOutput()}.
 *
 * <p>The output writer creates parent directories automatically before writing, so the
 * configured {@code src} path does not need to pre-exist.
 *
 * <p>This class is mapped from the {@code <output>} element in the pipeline XML by JAXB.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Output {

    private static final Logger logger = Logger.getLogger(Output.class.getName());

    @XmlAttribute
    private String type;

    @XmlAttribute
    private String ref;

    @XmlElement(name = "param")
    private List<Param> params = new ArrayList<>();

    private transient Map<String, String> resolvedParams = new HashMap<>();

    /**
     * Resolves this output against the global datasource catalogue.
     *
     * <p>Follows the same merge semantics as
     * {@link org.example.datapipeline.config.input.Input#resolve}: datasource parameters
     * are applied first, then inline {@code <param>} elements override them.
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

    /** @return the I/O type string (e.g. {@code "csv"}) used to select the writer */
    public String getType() { return type; }

    /**
     * Returns the resolved value of a named parameter.
     *
     * @param name parameter key (e.g. {@code "src"})
     * @return parameter value or {@code null} if not defined
     */
    public String getParam(String name) { return resolvedParams.get(name); }

    /**
     * Convenience accessor for the {@code src} parameter, which holds the destination
     * file path for CSV-type outputs.
     *
     * @return the value of the {@code src} parameter, or {@code null} if not set
     */
    public String getSrc() {
        return getParam("src");
    }

    /**
     * Drains the given iterator and writes every row to the configured output destination.
     *
     * <p>Delegates to {@link org.example.datapipeline.executor.io.DataIORegistry} to select
     * the correct {@link org.example.datapipeline.executor.io.DataWriter} implementation,
     * then calls {@link org.example.datapipeline.executor.io.DataWriter#writeData} with the
     * resolved parameter map. The writer is responsible for creating parent directories
     * and flushing the file handle.
     *
     * <p>This method is called by {@link org.example.datapipeline.executor.PipelineExecutor}
     * after each task completes, unless the action declares
     * {@link org.example.datapipeline.executor.action.ActionExecutor#handlesOwnOutput()}.
     *
     * @param it the iterator to drain; must include the header row as its first element
     * @throws RuntimeException if the writer type is unsupported or an I/O error occurs
     */
    public void writeData(DataIterator it) {
        DataIORegistry.getWriter(this.type).writeData(it, resolvedParams);
    }
}