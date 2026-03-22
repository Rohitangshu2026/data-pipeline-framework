package org.example.datapipeline.executor.iterator;


import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Provides a streaming iterator over rows of a CSV file.
 *
 * Reads the file line by line using a buffered reader and converts
 * each line into a string array by splitting on commas. This enables
 * lazy, memory-efficient access to large datasets without loading the
 * entire file into memory.
 *
 * The iterator maintains internal state by prefetching the next line,
 * allowing hasNext() to determine availability without advancing the stream.
 *
 * Each call to next() returns the current row and advances the reader
 * to the next line in the file.
 *
 * This class serves as the primary data source for streaming-based
 * pipeline execution.
 */
public class CsvDataIterator implements DataIterator {

    private BufferedReader reader;
    private String nextLine;

    public CsvDataIterator(String path) {
        try {
            reader = new BufferedReader(new FileReader(path));
            nextLine = reader.readLine();
        } catch (Exception e) {
            throw new RuntimeException("Failed to open file: " + path, e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextLine != null;
    }

    @Override
    public String[] next() {
        try {
            String[] row = nextLine.split(",");
            nextLine = reader.readLine();
            return row;
        } catch (Exception e) {
            throw new RuntimeException("Error reading CSV", e);
        }
    }
}