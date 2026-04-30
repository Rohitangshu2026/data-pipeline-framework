package org.example.datapipeline.executor.io;

import java.util.Map;
import java.util.HashMap;

/**
 * Central registry for all {@link DataReader} and {@link DataWriter} implementations.
 *
 * <p>Provides static lookup methods used by {@link org.example.datapipeline.config.input.Input}
 * and {@link org.example.datapipeline.config.output.Output} to open and write data sources
 * by type string (e.g. {@code "csv"}, {@code "api"}).
 *
 * <h2>Built-in Registrations</h2>
 * <p>Registered in the static initialiser block:
 * <ul>
 *   <li>{@link CsvDataReader}  – reads CSV files from the local filesystem</li>
 *   <li>{@link ApiDataReader}  – fetches and parses JSON arrays from HTTP APIs</li>
 *   <li>{@link CsvDataWriter}  – writes CSV files to the local filesystem</li>
 * </ul>
 *
 * <h2>Extensibility</h2>
 * <p>{@link #registerReader(DataReader)} and {@link #registerWriter(DataWriter)} are public
 * static methods so that tests or application bootstrap code can register additional I/O
 * implementations (e.g. a Parquet reader, a JDBC writer) without modifying this class.
 *
 * <h2>Thread Safety</h2>
 * <p>The registry maps are populated during class initialisation and not modified afterwards
 * in normal usage. Concurrent reads are safe. Calling {@link #registerReader} or
 * {@link #registerWriter} from multiple threads after initialisation would require
 * synchronisation (not currently provided).
 */
public class DataIORegistry {
    private static final Map<String, DataReader> readerRegistry = new HashMap<>();
    private static final Map<String, DataWriter> writerRegistry = new HashMap<>();

    static {
        registerReader(new CsvDataReader());
        registerReader(new ApiDataReader());
        registerWriter(new CsvDataWriter());
    }

    /**
     * Registers a reader implementation under its own type key (lowercased).
     *
     * <p>An existing registration for the same type is silently replaced.
     *
     * @param reader the {@link DataReader} implementation to register; must not be {@code null}
     */
    public static void registerReader(DataReader reader) {
        readerRegistry.put(reader.getType().toLowerCase(), reader);
    }

    /**
     * Registers a writer implementation under its own type key (lowercased).
     *
     * <p>An existing registration for the same type is silently replaced.
     *
     * @param writer the {@link DataWriter} implementation to register; must not be {@code null}
     */
    public static void registerWriter(DataWriter writer) {
        writerRegistry.put(writer.getType().toLowerCase(), writer);
    }

    /**
     * Looks up the registered reader for the given type.
     *
     * @param type the input type string (case-insensitive)
     * @return the registered {@link DataReader}; never {@code null}
     * @throws RuntimeException if no reader is registered for the given type
     */
    public static DataReader getReader(String type) {
        DataReader r = readerRegistry.get(type.toLowerCase());
        if (r == null) throw new RuntimeException("Unsupported input type: " + type);
        return r;
    }

    /**
     * Looks up the registered writer for the given type.
     *
     * @param type the output type string (case-insensitive)
     * @return the registered {@link DataWriter}; never {@code null}
     * @throws RuntimeException if no writer is registered for the given type
     */
    public static DataWriter getWriter(String type) {
        DataWriter w = writerRegistry.get(type.toLowerCase());
        if (w == null) throw new RuntimeException("Unsupported output type: " + type);
        return w;
    }
}
