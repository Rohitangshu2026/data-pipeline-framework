package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import java.util.Map;

/**
 * Strategy interface for all data source readers.
 *
 * <p>Implementations encapsulate the logic for opening a specific type of data source
 * (CSV file, HTTP API, database, etc.) and returning a streaming
 * {@link org.example.datapipeline.executor.iterator.DataIterator} over its rows.
 *
 * <p>Registered implementations are looked up by type string in
 * {@link DataIORegistry} using {@link #getType()}. Built-in implementations:
 * <ul>
 *   <li>{@link CsvDataReader} – reads CSV files from the local filesystem</li>
 *   <li>{@link ApiDataReader} – fetches JSON from an HTTP endpoint and extracts an array</li>
 * </ul>
 *
 * <p>Implementations must be <b>stateless</b> singleton instances stored in the registry.
 * All per-execution state must live in the iterator returned by {@link #createIterator}.
 */
public interface DataReader {

    /**
     * Returns the type identifier used to look up this reader in the registry.
     *
     * <p>Must match the {@code type} attribute on the corresponding {@code <datasource>}
     * or {@code <input>} element in the pipeline XML (case-insensitive lookup).
     *
     * @return lowercase type string (e.g. {@code "csv"}, {@code "api"})
     */
    String getType();

    /**
     * Opens and returns a streaming iterator over the data source described by the given
     * parameter map.
     *
     * <p>The returned iterator must be lazy – it should not read the entire source into
     * memory, but instead fetch and return rows on demand via
     * {@link org.example.datapipeline.executor.iterator.DataIterator#next()}.
     *
     * @param params the resolved parameter map from the task's input or datasource config
     *               (e.g. {@code {"src" -> "path/to/file.csv"}})
     * @return a new iterator positioned before the header row
     * @throws RuntimeException if required parameters are missing or the source cannot be opened
     */
    DataIterator createIterator(Map<String, String> params);
}
