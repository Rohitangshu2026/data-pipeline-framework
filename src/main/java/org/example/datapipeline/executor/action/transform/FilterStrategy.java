package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that retains only rows satisfying a column predicate.
 *
 * <p>Required method parameters:
 * <ul>
 *   <li>{@code column}   – name of the column to evaluate</li>
 *   <li>{@code operator} – comparison operator: {@code =}, {@code >}, {@code <},
 *       {@code >=}, {@code <=}</li>
 *   <li>{@code value}    – the right-hand side value to compare against</li>
 * </ul>
 *
 * <p>When both the cell value and the configured {@code value} are parseable as
 * {@code double}, the comparison is numeric. Otherwise it falls back to string equality
 * (only {@code =} makes sense for strings; other operators throw).
 *
 * <p>The returned iterator is <em>lazy</em>: it scans the upstream source one row at a
 * time in {@link DataIterator#hasNext()}, buffering only the next qualifying row.
 * Memory usage is O(1) regardless of dataset size.
 *
 * <p>The header row is always passed through unchanged as the first element.
 */
public class FilterStrategy implements TransformStrategy {

    /**
     * Creates a lazy filtering iterator over the given input.
     *
     * @param input  the upstream iterator (header row first)
     * @param method method configuration supplying {@code column}, {@code operator},
     *               and {@code value} parameters
     * @return a new iterator that yields the header followed by all rows that satisfy
     *         the configured predicate
     * @throws RuntimeException if {@code column} is not found in the header
     */
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
