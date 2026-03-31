package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import java.util.Map;

public class CsvDataReader implements DataReader {
    @Override
    public String getType() { return "csv"; }

    @Override
    public DataIterator createIterator(Map<String, String> params) {
        String src = params.get("src");
        if (src == null) throw new RuntimeException("Missing 'src' parameter for CSV input");
        return new CsvDataIterator(src);
    }
}
