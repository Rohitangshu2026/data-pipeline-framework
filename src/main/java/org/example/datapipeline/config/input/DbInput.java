package org.example.datapipeline.config.input;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;

/**
 * Represents a database-based input source for a task.
 *
 * Defines the connection details and query required to fetch data
 * from a database during pipeline execution. The 'connection'
 * attribute specifies the database connection string, while the
 * 'query' attribute defines the SQL query to be executed.
 *
 * This class is mapped from a <db> element under the input section
 * in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DbInput{

    @XmlAttribute
    private String connection;

    @XmlAttribute
    private String query;

    public String getConnection(){
        return connection;
    }
    public String getQuery(){
        return query;
    }
}