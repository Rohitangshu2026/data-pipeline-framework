package org.example.datapipeline.config.output;

import jakarta.xml.bind.annotation.*;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;

/**
 * Represents the output destination for a pipeline task.
 *
 * Supports multiple output types such as CSV files and databases,
 * as defined in the pipeline configuration. Provides utilities to
 * identify the output type, resolve the destination location, and
 * write processed data to the configured target.
 *
 * For CSV outputs:
 * - Data is written incrementally using a streaming iterator
 * - Each row is serialized as a comma-separated record
 * - Output directories are created automatically if they do not exist
 *
 * Streaming-based writing enables efficient handling of large datasets
 * without requiring the entire result to be held in memory.
 *
 * Database output support is defined but not yet implemented.
 *
 * This class serves as the final data emission layer of the framework.
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

    private void writeCsv(DataIterator it) {

        try {
            File file = new File(csv.getSrc());
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();   // ✅ safe directory creation
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {

                int count = 0;

                while (it.hasNext()) {
                    String[] row = it.next();

                    if (row == null) {
                        throw new RuntimeException("Iterator returned null row");
                    }

                    writer.write(String.join(",", row));
                    writer.newLine();

                    count++;
                }

                System.out.println("Rows written: " + count);

            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to write CSV: " + csv.getSrc(), e);
        }
    }

    public void writeData(DataIterator it) {

        if (isCsv()) {
            writeCsv(it);
            return;
        }

        throw new RuntimeException("No valid output defined");
    }

}