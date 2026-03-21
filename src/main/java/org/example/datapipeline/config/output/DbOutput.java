package org.example.datapipeline.config.output;

import jakarta.xml.bind.annotation.*;

/**
 * Represents a database-based output destination for a task.
 *
 * Defines the connection details and target table where the
 * processed data should be written during pipeline execution.
 * The 'connection' attribute specifies the database connection
 * string, and the 'table' attribute indicates the destination
 * table.
 *
 * This class is mapped from a <db> element under the output
 * section in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DbOutput{

    @XmlAttribute
    private String connection;

    @XmlAttribute
    private String table;

    public String getConnection(){
        return connection;
    }
    public String getTable(){
        return table;
    }
}