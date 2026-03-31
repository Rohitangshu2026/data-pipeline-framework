package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import java.util.Map;

public interface DataWriter {
    String getType();
    void writeData(DataIterator it, Map<String, String> params);
}
