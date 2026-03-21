package org.example.datapipeline.executor.iterator;

public interface DataIterator {
    boolean hasNext();
    String[] next();
}
