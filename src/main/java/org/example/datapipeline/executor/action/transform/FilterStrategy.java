package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class FilterStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();

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
