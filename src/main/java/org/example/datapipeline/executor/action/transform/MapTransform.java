package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.Map;

/**
 * Applies a row-wise transformation to a numeric column in a streaming dataset.
 *
 * Reads input data lazily using an iterator and updates the specified column
 * by applying an arithmetic operation with a constant value. The header row
 * is preserved and passed through unchanged.
 *
 * Supported operations:
 * - add       : adds a constant value
 * - subtract  : subtracts a constant value
 * - multiply  : multiplies by a constant value
 * - divide    : divides by a constant value
 *
 * The method expects parameters for:
 * - column    : column to be transformed
 * - operation : arithmetic operation to apply
 * - value     : numeric value used in the operation
 *
 * Each row is processed independently, and only the target column is modified.
 * Rows are cloned before modification to avoid mutating the original input.
 *
 * The transformation is performed in a streaming manner, enabling efficient
 * processing of large datasets without loading the entire input into memory.
 *
 * The result is returned as an iterator that yields:
 * - the original header row
 * - transformed data rows
 */
public class MapTransform implements TransformMethod {

    @Override
    public DataIterator apply(DataIterator input, ExecutionContext ctx) {

        Map<String, String> params = ctx.getMethod().getParamMap();

        String column = params.get("column");
        String operation = params.get("operation");
        String value = params.get("value");

        if (column == null || operation == null || value == null) {
            throw new RuntimeException("Missing params for map");
        }

        double val;
        try {
            val = Double.parseDouble(value);
        } catch (Exception e) {
            throw new RuntimeException("Invalid numeric value: " + value);
        }

        return new DataIterator() {

            String[] header;
            int colIndex = -1;
            boolean headerProcessed = false;

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public String[] next() {

                if (!headerProcessed) {
                    header = input.next();
                    colIndex = getColumnIndex(header, column);
                    headerProcessed = true;
                    return header;
                }

                String[] row = input.next().clone();

                if (colIndex >= row.length) {
                    return row;
                }

                try {
                    double num = Double.parseDouble(row[colIndex]);

                    switch (operation) {
                        case "add" -> num += val;
                        case "subtract" -> num -= val;
                        case "multiply" -> num *= val;
                        case "divide" -> num /= val;
                        default -> throw new RuntimeException("Invalid operation: " + operation);
                    }

                    row[colIndex] = String.valueOf(num);

                } catch (NumberFormatException e) {
                    throw new RuntimeException("Map supports numeric values only");
                }

                return row;
            }
        };
    }

    private int getColumnIndex(String[] header, String column) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equalsIgnoreCase(column)) return i;
        }
        throw new RuntimeException("Column not found: " + column);
    }
}