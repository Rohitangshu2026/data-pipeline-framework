package org.example.datapipeline.config.input;

import jakarta.xml.bind.annotation.*;

/**
 * Represents a CSV-based input source for a task.
 *
 * Defines the location of a CSV file that will be used as input
 * during pipeline execution. The source path is provided via the
 * 'src' attribute in the pipeline configuration.
 *
 * This class is mapped from a <csv> element under the input section
 * in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class CsvInput {

    @XmlAttribute
    private String src;

    public String getSrc() {
        return src;
    }
}