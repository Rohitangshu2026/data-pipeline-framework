package org.example.datapipeline.executor.iterator;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
public class CsvDataIterator implements DataIterator, AutoCloseable {

    private BufferedReader reader;
    private String nextLine;

    /**
     * Opens the given CSV file and pre-fetches the first line.
     *
     * <p>The file is opened with a {@link java.io.FileInputStream} wrapped in an
     * {@link java.io.InputStreamReader} with explicit UTF-8 encoding to correctly handle
     * files produced on any platform. The first line is read eagerly so that
     * {@link #hasNext()} can return the correct result immediately without requiring a
     * separate priming call.
     *
     * @param path the file system path to the CSV file (relative or absolute)
     * @throws RuntimeException if the file does not exist or cannot be opened
     */
    public CsvDataIterator(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(path), StandardCharsets.UTF_8));
            nextLine = reader.readLine();
        } catch (Exception e) {
            throw new RuntimeException("Failed to open file: " + path, e);
        }
    }

    /**
     * Returns {@code true} if there is another line available to read.
     *
     * <p>Automatically calls {@link #close()} when the last line has been consumed so
     * that the underlying file handle is released without requiring an explicit close call
     * from the consumer.
     *
     * @return {@code true} if {@link #next()} will succeed, {@code false} if EOF has been reached
     */
    @Override
    public boolean hasNext() {
        if (nextLine != null) return true;
        close();
        return false;
    }

    /**
     * Closes the underlying {@link java.io.BufferedReader} and releases the file handle.
     *
     * <p>Safe to call multiple times — subsequent calls after the first are no-ops.
     * Any {@link IOException} from the close call is silently ignored.
     */
    @Override
    public void close() {
        try {
            if (reader != null) { reader.close(); reader = null; }
        } catch (IOException ignored) {}
    }

    /**
     * Returns the current row (as a {@code String[]} split on commas) and advances to
     * the next line.
     *
     * <p>The split uses {@code -1} as the limit argument so that trailing empty fields
     * (e.g. a row ending with {@code ","}) are preserved as empty strings rather than
     * being discarded.
     *
     * @return the current row's values; never {@code null} when called after
     *         {@link #hasNext()} returned {@code true}
     * @throws RuntimeException if called when no more lines are available, or on I/O error
     */
    @Override
    public String[] next() {
        try {
            if (nextLine == null) {
                throw new RuntimeException("CsvDataIterator next called when hasNext is false");
            }
            String[] row = nextLine.split(",", -1);
            nextLine = (reader != null) ? reader.readLine() : null;
            return row;
        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV", e);
        }
    }
}