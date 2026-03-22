package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class DropNullsStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String columnsParam = params.get("columns");
        if (columnsParam == null) {
            throw new RuntimeException("Missing params for drop_nulls");
        }

        String[] colsToDrop = Arrays.stream(columnsParam.split(","))
                .map(String::trim).toArray(String[]::new);

        return new DataIterator() {

            boolean headerProcessed = false;
            int[] indices;
            String[] nextRow = null;

            @Override
            public boolean hasNext() {
                if (!headerProcessed) return input.hasNext();
                if (nextRow != null) return true;

                while (input.hasNext()) {
                    String[] row = input.next();
                    boolean hasNull = false;
                    for (int idx : indices) {
                        if (idx >= row.length || row[idx] == null || row[idx].trim().isEmpty()) {
                            hasNull = true;
                            break;
                        }
                    }
                    if (!hasNull) {
                        nextRow = row;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] header = input.next();
                    headerProcessed = true;
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < header.length; i++) map.put(header[i].trim(), i);
                    indices = new int[colsToDrop.length];
                    for (int i = 0; i < colsToDrop.length; i++) {
                        Integer idx = map.get(colsToDrop[i]);
                        indices[i] = idx == null ? -1 : idx;
                    }
                    return header;
                }
                if (nextRow != null || hasNext()) {
                    String[] res = nextRow;
                    nextRow = null;
                    return res;
                }
                throw new RuntimeException("No more elements");
            }
        };
    }
}
