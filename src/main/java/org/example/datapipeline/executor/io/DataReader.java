package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import java.util.Map;

public interface DataReader {
    String getType();
    DataIterator createIterator(Map<String, String> params);
}
