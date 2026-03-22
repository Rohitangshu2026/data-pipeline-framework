package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class FillNullsStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String column = params.get("column");
        String value = params.get("value");

        if (column == null || value == null) {
            throw new RuntimeException("Missing params for fill_nulls");
        }

        return new DataIterator() {

            boolean headerProcessed = false;
            int colIndex = -1;

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] header = input.next();
                    headerProcessed = true;
                    for (int i = 0; i < header.length; i++) {
                        if (header[i].trim().equals(column)) {
                            colIndex = i; break;
                        }
                    }
                    return header;
                }
                
                String[] row = input.next().clone();
                if (colIndex != -1 && colIndex < row.length) {
                    if (row[colIndex] == null || row[colIndex].trim().isEmpty()) {
                        row[colIndex] = value;
                    }
                }
                return row;
            }
        };
    }
}
