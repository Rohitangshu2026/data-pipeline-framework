package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class SelectStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();

        String columnsParam = params.get("columns");
        if (columnsParam == null) {
            throw new RuntimeException("Missing 'columns' param for select");
        }

        String[] requiredCols = Arrays.stream(columnsParam.split(","))
                .map(String::trim)
                .toArray(String[]::new);

        return new DataIterator() {

            String[] header;
            int[] indices;
            boolean headerProcessed = false;

            String[] nextRow = null;
            boolean fetched = false;

            @Override
            public boolean hasNext() {

                if (!headerProcessed) {
                    return input.hasNext();
                }

                if (fetched) return nextRow != null;

                if (input.hasNext()) {
                    nextRow = input.next();
                    fetched = true;
                    return true;
                }

                nextRow = null;
                fetched = true;
                return false;
            }

            @Override
            public String[] next() {

                if (!headerProcessed) {
                    if (!input.hasNext()) {
                        throw new RuntimeException("Empty input data");
                    }

                    header = input.next();

                    Map<String, Integer> colIndexMap = new HashMap<>();
                    for (int i = 0; i < header.length; i++) {
                        colIndexMap.put(header[i].trim(), i);
                    }

                    indices = new int[requiredCols.length];

                    for (int i = 0; i < requiredCols.length; i++) {
                        Integer idx = colIndexMap.get(requiredCols[i]);
                        if (idx == null) {
                            throw new RuntimeException("Column not found: " + requiredCols[i]);
                        }
                        indices[i] = idx;
                    }

                    headerProcessed = true;
                    return requiredCols;
                }

                if (!fetched) {
                    if (!hasNext()) {
                        throw new RuntimeException("No more elements");
                    }
                }

                if (nextRow == null) {
                    throw new RuntimeException("No more elements");
                }

                String[] row = nextRow;
                nextRow = null;
                fetched = false;

                String[] newRow = new String[indices.length];

                for (int i = 0; i < indices.length; i++) {
                    int idx = indices[i];
                    newRow[i] = idx < row.length ? row[idx] : "";
                }

                return newRow;
            }
        };
    }
}
