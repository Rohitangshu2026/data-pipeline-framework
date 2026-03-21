package org.example.datapipeline.config.output;

import jakarta.xml.bind.annotation.*;

/**
 * Represents a CSV-based output destination for a task.
 *
 * Defines the file path where the processed data will be written
 * during pipeline execution. The destination path is provided via
 * the 'src' attribute in the pipeline configuration.
 *
 * This class is mapped from a <csv> element under the output
 * section in the pipeline XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class CsvOutput {

    @XmlAttribute
    private String src;

    public String getSrc() {
        return src;
    }
}