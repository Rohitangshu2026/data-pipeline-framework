package org.example.datapipeline.executor.metrics;

import org.example.datapipeline.executor.iterator.DataIterator;

public class CountingIterator implements DataIterator {

    private final DataIterator inner;
    private long count = 0;

    public CountingIterator(DataIterator inner) {
        this.inner = inner;
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public String[] next() {
        String[] row = inner.next();
        count++;
        return row;
    }

    public long getCount() {
        return count;
    }
}