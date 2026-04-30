package org.example.datapipeline.executor.iterator;

public interface DataIterator extends AutoCloseable {
    boolean hasNext();
    String[] next();
    default void close() {}
}
