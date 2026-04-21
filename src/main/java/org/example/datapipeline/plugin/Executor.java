package org.example.datapipeline.plugin;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

public interface Executor {
    DataIterator execute(ExecutionContext context);
}
