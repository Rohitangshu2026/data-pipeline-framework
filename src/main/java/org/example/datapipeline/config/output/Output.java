package org.example.datapipeline.config.output;

import jakarta.xml.bind.annotation.*;

/**
 * Represents the output destination for a task.
 *
 * Supports multiple output types such as CSV files and databases.
 * Only one output type is expected to be defined at a time, and
 * the appropriate configuration is selected based on which element
 * is present in the pipeline XML.
 *
 * Provides helper methods to determine the output type and retrieve
 * the corresponding destination (file path or database table).
 *
 * This class is mapped from the <output> element in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Output{

    @XmlElement(name = "csv")
    private CsvOutput csv;

    @XmlElement(name = "db")
    private DbOutput db;

    public String getSrc() {
        if (csv != null) return csv.getSrc();
        if (db != null) return db.getTable(); // or connection depending on usage
        return null;
    }

    public boolean isCsv(){
        return csv != null;
    }
    public boolean isDb(){
        return db != null;
    }
}