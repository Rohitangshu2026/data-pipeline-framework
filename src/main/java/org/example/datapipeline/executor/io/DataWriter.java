package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import java.util.Map;

/**
 * Strategy interface for all data sink writers.
 *
 * <p>Implementations encapsulate the logic for draining a
 * {@link org.example.datapipeline.executor.iterator.DataIterator} and writing every row to
 * a specific type of output destination (CSV file, database, message queue, etc.).
 *
 * <p>Registered implementations are looked up by type string in {@link DataIORegistry}.
 * The only built-in implementation is {@link CsvDataWriter}.
 *
 * <p>Implementations must be <b>stateless</b> singleton instances. All per-write state
 * (file handles, buffers) must be scoped to the {@link #writeData} call.
 */
public interface DataWriter {

    /**
     * Returns the type identifier used to look up this writer in the registry.
     *
     * @return lowercase type string (e.g. {@code "csv"})
     */
    String getType();

    /**
     * Drains the given iterator and writes all rows to the output destination described by
     * the given parameter map.
     *
     * <p>The first row returned by the iterator is the header row; implementations should
     * write it as the first output row. The writer is responsible for:
     * <ul>
     *   <li>Creating any necessary parent directories.</li>
     *   <li>Opening and closing the output resource within this call.</li>
     *   <li>Flushing and closing file handles before returning.</li>
     * </ul>
     *
     * @param it     the iterator to drain; the first row is the header
     * @param params the resolved parameter map from the task's output or datasource config
     *               (e.g. {@code {"src" -> "target/out.csv"}})
     * @throws RuntimeException if a required parameter is missing or an I/O error occurs
     */
    void writeData(DataIterator it, Map<String, String> params);
}
