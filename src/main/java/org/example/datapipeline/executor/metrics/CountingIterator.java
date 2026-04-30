package org.example.datapipeline.executor.metrics;

import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Transparent decorator iterator that counts the total number of rows emitted.
 *
 * <p>{@code CountingIterator} wraps any {@link DataIterator} and increments an internal
 * counter on every call to {@link #next()}, including the header row. It delegates all
 * other behaviour ({@link #hasNext()}, {@link #close()}) to the inner iterator unchanged.
 *
 * <p>Two instances are used per task in
 * {@link org.example.datapipeline.executor.PipelineExecutor}:
 * <ul>
 *   <li><b>Input counter</b> – wraps the raw source iterator; its count becomes
 *       {@code rowsIn} in {@link TaskMetrics} and
 *       {@link org.example.datapipeline.versioning.TaskRun}.</li>
 *   <li><b>Output counter</b> – wraps the transformed iterator (after the action executes);
 *       its count becomes {@code rowsOut}.</li>
 * </ul>
 *
 * <p>This design keeps metrics instrumentation completely separate from the data-processing
 * logic in action executors and transform strategies.
 */
public class CountingIterator implements DataIterator {

    private final DataIterator inner;
    private long count = 0;

    /**
     * Wraps the given iterator for transparent row counting.
     *
     * @param inner the upstream iterator to wrap; must not be {@code null}
     */
    public CountingIterator(DataIterator inner) {
        this.inner = inner;
    }

    /** Delegates to the inner iterator. */
    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    /**
     * Returns the next row from the inner iterator and increments the row counter.
     *
     * <p>The counter is incremented for every row, including the header row. Callers that
     * want only the data-row count should subtract 1 from the total, but the framework
     * currently reports the raw count (inclusive of header) in metrics.
     *
     * @return the next row from the inner iterator
     */
    @Override
    public String[] next() {
        String[] row = inner.next();
        count++;
        return row;
    }

    /**
     * Returns the total number of rows fetched via {@link #next()} so far.
     *
     * @return cumulative row count (includes header row if it has been consumed)
     */
    public long getCount() {
        return count;
    }

    /**
     * Closes the inner iterator, suppressing any exceptions to avoid masking upstream errors.
     */
    @Override
    public void close() {
        try {
            inner.close();
        } catch (Exception ignored) {}
    }
}