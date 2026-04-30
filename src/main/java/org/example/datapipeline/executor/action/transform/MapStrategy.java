package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.Map;

/**
 * Transform strategy that applies a scalar arithmetic operation to every value in one column.
 *
 * <p>Required method parameters:
 * <ul>
 *   <li>{@code column}    – name of the column to transform</li>
 *   <li>{@code operation} – arithmetic operation to apply: {@code add}, {@code subtract},
 *       {@code multiply}, {@code divide}</li>
 *   <li>{@code value}     – numeric constant (parsed as {@code double}) to use as the
 *       right-hand operand</li>
 * </ul>
 *
 * <p>The column value is parsed as {@code double}, the operation is applied, and the result
 * is written back as a string in-place. Non-numeric values throw a
 * {@link RuntimeException}. The column name and all other columns are passed through
 * unchanged.
 *
 * <p>Typical use case: currency conversion ({@code multiply × 0.92} for USD→EUR).
 *
 * <p>The returned iterator is <em>lazy</em>: each row is cloned and the target cell is
 * mutated on-the-fly during iteration without buffering the full dataset.
 */
public class MapStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();

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
