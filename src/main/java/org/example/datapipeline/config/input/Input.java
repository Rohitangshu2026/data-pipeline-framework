package org.example.datapipeline.config.input;

import jakarta.xml.bind.annotation.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the input source for a task in a pipeline stage.
 *
 * Supports multiple input types such as CSV files and databases,
 * as defined in the pipeline XML configuration.
 *
 * Provides helper methods to:
 * - Identify the input type (CSV or DB)
 * - Retrieve the source location
 * - Read input data into an in-memory structure for processing
 *
 * For CSV inputs, the data is loaded as a list of rows,
 * where each row is represented as a string array.
 *
 * Database input support is defined but not yet implemented.
 *
 * This class acts as the data extraction layer in the ETL pipeline.
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

    public List<String[]> readData() {

        if (isCsv()) {
            return readCsv();
        }

        if (isDb()) {
            throw new RuntimeException("DB input not implemented yet");
        }

        throw new RuntimeException("No valid input source");
    }

    private List<String[]> readCsv() {

        List<String[]> data = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(csv.getSrc()))) {

            String line;

            while ((line = reader.readLine()) != null) {
                data.add(line.split(","));
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to read CSV: " + csv.getSrc(), e);
        }

        return data;
    }
}