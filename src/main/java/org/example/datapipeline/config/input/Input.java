package org.example.datapipeline.config.input;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.iterator.CsvDataIterator;

import jakarta.xml.bind.annotation.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the input source for a pipeline task.
 *
 * Supports multiple input types such as CSV files and databases,
 * as defined in the pipeline configuration. Provides utilities to
 * identify the input type, retrieve the source location, and access
 * data either in-memory or as a stream.
 *
 * For CSV inputs:
 * - Data can be fully loaded into memory as a list of rows
 * - Data can be consumed lazily using a streaming iterator
 *
 * Streaming mode enables efficient processing of large datasets
 * without loading the entire file into memory, making it suitable
 * for scalable pipeline execution.
 *
 * Database input support is defined but not yet implemented.
 *
 * This class acts as the entry point for data ingestion within the framework.
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

    public DataIterator streamData() {
        if (isCsv()) {
            return new CsvDataIterator(csv.getSrc());
        }
        throw new RuntimeException("Streaming not supported for this input type");
    }
}