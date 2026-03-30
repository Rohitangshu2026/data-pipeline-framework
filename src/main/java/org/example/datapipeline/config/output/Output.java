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
import java.io.FileWriter;
import java.io.File;

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

    private void writeCsv(DataIterator it) {
        String src = getSrc();
        try {
            File file = new File(src);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                int count = 0;
                while (it.hasNext()) {
                    String[] row = it.next();
                    if (row == null) throw new RuntimeException("Iterator returned null row");
                    writer.write(String.join(",", row));
                    writer.newLine();
                    count++;
                }
                logger.info("ROWS_WRITTEN count=" + count + " file=" + src);
            }
        } catch (Exception e) {
            logger.severe(String.format("CSV_WRITE_FAILED file=%s error=%s", src, e.getMessage()));
            throw new RuntimeException("Failed to write CSV: " + src, e);
        }
    }

    public void writeData(DataIterator it) {
        if ("csv".equals(type)) {
            writeCsv(it);
            return;
        }
        throw new RuntimeException("No valid output defined for type: " + type);
    }
}