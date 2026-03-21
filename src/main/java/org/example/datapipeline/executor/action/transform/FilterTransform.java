package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;

import java.util.*;

/**
 * Filters rows from the dataset based on a condition applied to a specific column.
 *
 * Evaluates each row against a comparison condition and retains only
 * those rows that satisfy the condition. The header row is preserved.
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
 * Numeric comparisons are performed when possible. If parsing fails,
 * a string equality check is used.
 *
 * Input data is read from the execution context and filtered in memory.
 * The resulting dataset replaces the original data in the context.
 */
public class FilterTransform implements TransformMethod {

    @Override
    public void apply(ExecutionContext ctx) {

        List<String[]> data = ctx.getData();
        Map<String, String> params = ctx.getMethod().getParamMap();

        String column = params.get("column");
        String operator = params.get("operator");
        String value = params.get("value");

        String[] header = data.get(0);
        int colIndex = getColumnIndex(header, column);

        List<String[]> result = new ArrayList<>();
        result.add(header);

        for (int i = 1; i < data.size(); i++) {
            String[] row = data.get(i);

            if (evaluate(row[colIndex], operator, value)) {
                result.add(row);
            }
        }

        ctx.setData(result);
    }

    private int getColumnIndex(String[] header, String column) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equals(column)) return i;
        }
        throw new RuntimeException("Column not found: " + column);
    }

    private boolean evaluate(String cell, String operator, String value) {
        try {
            double c = Double.parseDouble(cell);
            double v = Double.parseDouble(value);

            return switch (operator) {
                case ">" -> c > v;
                case "<" -> c < v;
                case "=" -> c == v;
                case ">=" -> c >= v;
                case "<=" -> c <= v;
                default -> throw new RuntimeException("Invalid operator: " + operator);
            };

        } catch (NumberFormatException e) {
            return cell.equals(value);
        }
    }
}