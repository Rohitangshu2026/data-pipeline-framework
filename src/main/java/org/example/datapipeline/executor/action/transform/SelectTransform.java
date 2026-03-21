package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;

import java.util.*;

/**
 * Selects a subset of columns from the dataset and discards the rest.
 *
 * Projects each row onto a specified set of columns while preserving
 * the order defined in the configuration. The header is updated to
 * reflect only the selected columns.
 *
 * The method expects parameters for:
 * - columns : comma-separated list of column names to retain
 *
 * For each row, values corresponding to the selected columns are extracted
 * and arranged in the specified order. If a column is missing in the header,
 * execution fails.
 *
 * Input data is read from the execution context and transformed in memory.
 * The resulting dataset replaces the original data in the context.
 */
public class SelectTransform implements TransformMethod {

    @Override
    public void apply(ExecutionContext ctx) {

        List<String[]> data = ctx.getData();
        Map<String, String> params = ctx.getMethod().getParamMap();

        if (data == null || data.isEmpty()) {
            throw new RuntimeException("No data available for select");
        }

        String columnsParam = params.get("columns");
        if (columnsParam == null) {
            throw new RuntimeException("Missing 'columns' param for select");
        }

        String[] requiredCols = columnsParam.split(",");

        String[] header = data.get(0);

        Map<String, Integer> colIndexMap = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            colIndexMap.put(header[i].trim(), i);
        }

        List<String[]> result = new ArrayList<>();

        // new header
        String[] newHeader = Arrays.stream(requiredCols)
                .map(String::trim)
                .toArray(String[]::new);

        result.add(newHeader);

        for (int i = 1; i < data.size(); i++) {

            String[] row = data.get(i);
            String[] newRow = new String[newHeader.length];

            for (int j = 0; j < newHeader.length; j++) {
                String col = newHeader[j];

                Integer idx = colIndexMap.get(col);
                if (idx == null) {
                    throw new RuntimeException("Column not found: " + col);
                }

                newRow[j] = idx < row.length ? row[idx] : "";
            }

            result.add(newRow);
        }

        ctx.setData(result);
    }
}