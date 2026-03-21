package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Projects a subset of columns from a streaming dataset.
 *
 * Consumes input rows lazily and emits only the specified columns
 * in the order defined by the configuration. The header is transformed
 * to include only the selected columns.
 *
 * The method expects parameters for:
 * - columns : comma-separated list of column names to retain
 *
 * During initialization, the header row is used to resolve column indices.
 * If any requested column is not found, execution fails immediately.
 *
 * Each subsequent row is transformed by extracting values corresponding
 * to the selected columns. Missing values are replaced with empty strings.
 *
 * The transformation is performed in a streaming manner with minimal buffering,
 * ensuring efficient processing of large datasets without loading the entire
 * input into memory.
 *
 * The result is returned as an iterator that yields:
 * - a new header containing only selected columns
 * - projected data rows with values in the specified order
 */
public class SelectTransform implements TransformMethod {

    @Override
    public DataIterator apply(DataIterator input, ExecutionContext ctx) {

        Map<String, String> params = ctx.getMethod().getParamMap();

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

                // HEADER
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