package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import java.util.Map;

/**
 * {@link DataReader} implementation that opens a CSV file from the local filesystem.
 *
 * <p>Registered in {@link DataIORegistry} under the type key {@code "csv"}. Used whenever
 * an {@code <input type="csv">} or a {@code <datasource type="csv">} is encountered in the
 * pipeline XML.
 *
 * <p>Required parameter:
 * <ul>
 *   <li>{@code src} – path to the CSV file (relative to the working directory or absolute)</li>
 * </ul>
 *
 * <p>Delegates to {@link CsvDataIterator} which opens a {@link java.io.BufferedReader} over
 * a UTF-8 {@link java.io.FileInputStream} for memory-efficient line-by-line streaming.
 */
public class CsvDataReader implements DataReader {

    /** @return {@code "csv"} – the type string for this reader */
    @Override
    public String getType() { return "csv"; }

    /**
     * Creates a streaming iterator over the CSV file at the path given by the {@code src}
     * parameter.
     *
     * @param params parameter map containing at least {@code src}
     * @return a new {@link CsvDataIterator} positioned before the header row
     * @throws RuntimeException if {@code src} is not present in the parameter map
     */
    @Override
    public DataIterator createIterator(Map<String, String> params) {
        String src = params.get("src");
        if (src == null) throw new RuntimeException("Missing 'src' parameter for CSV input");
        return new CsvDataIterator(src);
    }
}
