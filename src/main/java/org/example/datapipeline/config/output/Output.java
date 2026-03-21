package org.example.datapipeline.config.output;

import jakarta.xml.bind.annotation.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

/**
 * Represents the output destination for a task in a pipeline stage.
 *
 * Supports multiple output types such as CSV files and databases,
 * as defined in the pipeline XML configuration.
 *
 * Provides helper methods to:
 * - Identify the output type (CSV or DB)
 * - Retrieve the destination location
 * - Write processed data from memory to the configured output
 *
 * For CSV outputs, the data is written row by row,
 * where each row is represented as a string array.
 *
 * Database output support is defined but not yet implemented.
 *
 * This class acts as the data loading layer in the ETL pipeline.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Output{

    @XmlElement(name = "csv")
    private CsvOutput csv;

    @XmlElement(name = "db")
    private DbOutput db;

    public String getSrc() {
        if (csv != null) return csv.getSrc();
        if (db != null) return db.getTable();
        return null;
    }

    public boolean isCsv(){
        return csv != null;
    }
    public boolean isDb(){
        return db != null;
    }

    public void writeData(List<String[]> data) {

        if (isCsv()) {
            writeCsv(data);
            return;
        }

        if (isDb()) {
            throw new RuntimeException("DB output not implemented yet");
        }

        throw new RuntimeException("No valid output defined");
    }

    private void writeCsv(List<String[]> data) {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csv.getSrc()))) {

            for (String[] row : data) {
                writer.write(String.join(",", row));
                writer.newLine();
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to write CSV: " + csv.getSrc(), e);
        }
    }
}