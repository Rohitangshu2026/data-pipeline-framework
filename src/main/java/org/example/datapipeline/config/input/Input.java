package org.example.datapipeline.config.input;

import jakarta.xml.bind.annotation.*;

/**
 * Represents the input source for a task in a pipeline stage.
 *
 * The input defines the source dataset that the task
 * will consume during execution. This value is specified in the
 * pipeline XML configuration.
 *
 * This class is mapped from the <input> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Input {

    @XmlElement(name = "csv")
    private CsvInput csv;

    @XmlElement(name = "db")
    private DbInput db;

    public String getSrc(){
        if (csv != null) return csv.getSrc();
        if (db != null) return db.getConnection(); // or query depending on use
        return null;
    }

    public boolean isCsv(){
        return csv != null;
    }

    public boolean isDb(){
        return db != null;
    }
}