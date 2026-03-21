package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Defines a contract for implementing transformation operations
 * within the pipeline.
 *
 * Each transform method processes data using the provided execution
 * context and updates the dataset in place.
 *
 * Implementations encapsulate specific transformation logic such as
 * filtering, projection, mapping, or aggregation.
 */
public interface TransformMethod {
    DataIterator apply(DataIterator input, ExecutionContext ctx);
}