package org.example.datapipeline.executor.iterator;

/**
 * Core streaming abstraction for all data flowing through the pipeline.
 *
 * <p>Every stage in the pipeline reads from and writes to a {@code DataIterator}.
 * Transform strategies wrap an upstream {@code DataIterator} in a new anonymous
 * implementation that applies a transformation lazily on each {@link #next()} call.
 * This creates an <em>iterator chain</em>: reading one row from the final iterator
 * pulls one row through every transform in the chain without ever materialising the
 * full dataset in memory.
 *
 * <p>The contract mirrors {@link java.util.Iterator} with the addition of
 * {@link AutoCloseable} so that underlying file handles or network connections can
 * be released deterministically:
 * <ul>
 *   <li>{@link #hasNext()} must be called before each {@link #next()} call.</li>
 *   <li>{@link #next()} returns the current row as a {@code String[]} and advances
 *       the cursor. The first row returned is always the header row.</li>
 *   <li>{@link #close()} is a no-op by default; leaf iterators (e.g.
 *       {@link CsvDataIterator}) override it to close their underlying reader.</li>
 * </ul>
 *
 * <p>All implementations of this interface must be <em>single-threaded</em>.
 * The pipeline executor invokes iterators from a single thread per task; only
 * different tasks may run concurrently, and each task owns its own iterator chain.
 *
 * <p><b>Row format:</b> every {@code String[]} represents one CSV-like row, where
 * element {@code [0]} is the first column value. The header row contains column names
 * in the same positional order. Implementations must always return the header as the
 * very first row.
 */
public interface DataIterator extends AutoCloseable {

    /**
     * Returns {@code true} if there is at least one more row available (including
     * the header row on the very first call).
     *
     * <p>Implementations that perform look-ahead (e.g. {@link CsvDataIterator}) should
     * prefetch in this method so that {@link #next()} never blocks.
     *
     * @return {@code true} if calling {@link #next()} will succeed
     */
    boolean hasNext();

    /**
     * Returns the next row and advances the cursor.
     *
     * <p>The first call always returns the header row. Subsequent calls return data rows
     * in the order they appear in the source. The caller must check {@link #hasNext()}
     * before calling this method.
     *
     * @return a {@code String[]} where each element is a column value (never {@code null},
     *         but individual elements may be empty strings for missing/null values)
     * @throws RuntimeException if there are no more elements or if an I/O error occurs
     */
    String[] next();

    /**
     * Releases any resources held by this iterator (file handles, network connections, etc.).
     *
     * <p>The default implementation is a no-op. Leaf iterators that own an I/O resource
     * (e.g. {@link CsvDataIterator}) must override this method. Wrapping iterators in the
     * transform chain do not need to override it because the framework drains and closes
     * the chain via the leaf.
     */
    default void close() {}
}
