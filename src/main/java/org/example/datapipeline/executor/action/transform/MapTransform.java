package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;

import java.util.*;

/**
 * Applies a transformation to a numeric column across all rows in the dataset.
 *
 * Performs a row-wise operation on a specified column by modifying its value
 * using a given arithmetic operation and constant value. The header row is preserved.
 *
 * Supported operations:
 * - add       : adds a value to the column
 * - subtract  : subtracts a value from the column
 * - multiply  : multiplies the column by a value
 * - divide    : divides the column by a value
 *
 * The method expects parameters for:
 * - column    : column to be transformed
 * - operation : arithmetic operation to apply
 * - value     : numeric value used in the operation
 *
 * Only numeric columns are supported. If a value cannot be parsed as a number,
 * execution fails.
 *
 * Input data is read from the execution context and transformed in memory.
 * The resulting dataset replaces the original data in the context.
 */
public class MapTransform implements TransformMethod {

    @Override
    public void apply(ExecutionContext ctx) {

        List<String[]> data = ctx.getData();
        Map<String, String> params = ctx.getMethod().getParamMap();

        if (data == null || data.isEmpty()) {
            throw new RuntimeException("No data available for map");
        }

        String column = params.get("column");
        String operation = params.get("operation");
        String value = params.get("value");

        if (column == null || operation == null || value == null) {
            throw new RuntimeException("Missing params for map");
        }

        String[] header = data.get(0);

        int colIndex = -1;
        for (int i = 0; i < header.length; i++) {
            if (header[i].equals(column)) {
                colIndex = i;
                break;
            }
        }

        if (colIndex == -1) {
            throw new RuntimeException("Column not found: " + column);
        }

        List<String[]> result = new ArrayList<>();
        result.add(header);

        for (int i = 1; i < data.size(); i++) {

            String[] row = data.get(i).clone();

            try {
                double num = Double.parseDouble(row[colIndex]);
                double val = Double.parseDouble(value);

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

            result.add(row);
        }

        ctx.setData(result);
    }
}