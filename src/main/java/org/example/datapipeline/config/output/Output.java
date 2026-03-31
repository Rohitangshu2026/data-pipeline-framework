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

    public void resolve(Map<String, Datasource> globals) {
        if (ref != null && globals.containsKey(ref)) {
            Datasource ds = globals.get(ref);
            if (this.type == null) this.type = ds.getType();
            for (Param p : ds.getParams()) resolvedParams.put(p.getName(), p.getValue());
        }
        for (Param p : params) resolvedParams.put(p.getName(), p.getValue());
    }

    public String getType() { return type; }
    public String getParam(String name) { return resolvedParams.get(name); }

    public String getSrc() {
        return getParam("src");
    }

    public void writeData(DataIterator it) {
        DataIORegistry.getWriter(this.type).writeData(it, resolvedParams);
    }
}