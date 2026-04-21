package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.iterator.ApiDataIterator;
import java.util.Map;

public class ApiDataReader implements DataReader {
    @Override
    public String getType() { return "api"; }

    @Override
    public DataIterator createIterator(Map<String, String> params) {
        String url = params.get("url");
        String jsonPath = params.get("json_path");
        String fields = params.get("fields");

        if (url == null) throw new RuntimeException("Missing 'url' parameter for API input");
        if (jsonPath == null) throw new RuntimeException("Missing 'json_path' parameter for API input");
        if (fields == null) throw new RuntimeException("Missing 'fields' parameter for API input (comma separated columns to extract)");

        return new ApiDataIterator(url, jsonPath, fields);
    }
}
