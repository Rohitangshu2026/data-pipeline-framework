package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Defines a contract for implementing transformation operations.
 * Allows strategy-based implementation of data pipeline transforms.
 */
public interface TransformStrategy {
    DataIterator apply(DataIterator input, Method method);
}
