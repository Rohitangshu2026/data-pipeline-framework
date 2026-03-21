package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Filters rows from a streaming dataset based on a condition applied to a specific column.
 *
 * Processes input data lazily using an iterator and emits only those rows
 * that satisfy the given condition. The header row is preserved and passed through unchanged.
 *
 * Supported operators:
 * - >   : greater than (numeric)
 * - <   : less than (numeric)
 * - =   : equal to (numeric or string)
 * - >=  : greater than or equal to (numeric)
 * - <=  : less than or equal to (numeric)
 *
 * The method expects parameters for:
 * - column   : column on which the condition is applied
 * - operator : comparison operator
 * - value    : value to compare against
 *
 * Numeric comparisons are performed when both the cell value and comparison
 * value can be parsed as numbers. Otherwise, a string equality check is used.
 *
 * The transformation is applied in a streaming fashion, buffering only the
 * next valid row when needed. This enables efficient processing of large datasets
 * without loading the entire input into memory.
 *
 * The result is returned as a new iterator that yields:
 * - the original header row
 * - filtered data rows satisfying the condition
 */
public class FilterTransform implements TransformMethod {

    @Override
    public DataIterator apply(DataIterator input, ExecutionContext ctx) {

        Map<String, String> params = ctx.getMethod().getParamMap();

        String column = params.get("column");
        String operator = params.get("operator");
        String value = params.get("value");

        return new DataIterator() {

            String[] header;
            int colIndex = -1;
            boolean headerProcessed = false;
            String[] nextValid = null;

            @Override
            public boolean hasNext() {

                if (!headerProcessed) {
                    if (!input.hasNext()) return false;
                    return true;
                }

                if (nextValid != null) return true;

                while (input.hasNext()) {
                    String[] row = input.next();
                    if (colIndex >= row.length)
                        continue;
                    if (evaluate(row[colIndex], operator, value)) {
                        nextValid = row;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String[] next() {

                if (!headerProcessed) {
                    header = input.next();
                    colIndex = getColumnIndex(header, column);
                    headerProcessed = true;
                    return header;
                }

                if (nextValid != null || hasNext()) {
                    String[] temp = nextValid;
                    nextValid = null;
                    return temp;
                }

                throw new RuntimeException("No more elements");
            }
        };
    }

    private int getColumnIndex(String[] header, String column) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equals(column)) return i;
        }
        throw new RuntimeException("Column not found: " + column);
    }

    private boolean evaluate(String cell, String operator, String value) {

        Double c = tryParse(cell);
        Double v = tryParse(value);

        if (c != null && v != null) {
            return switch (operator) {
                case ">" -> c > v;
                case "<" -> c < v;
                case "=" -> c.equals(v);
                case ">=" -> c >= v;
                case "<=" -> c <= v;
                default -> throw new RuntimeException("Invalid operator: " + operator);
            };
        }

        return cell.equals(value);
    }
    private Double tryParse(String s) {
        try { return Double.parseDouble(s); }
        catch (Exception e) { return null; }
    }
}