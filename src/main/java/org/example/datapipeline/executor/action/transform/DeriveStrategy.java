package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class DeriveStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String newColumn = params.get("new_column");
        String formula = params.get("formula");

        if (newColumn == null || formula == null) {
            throw new RuntimeException("Missing params for derive");
        }

        return new DataIterator() {

            String[] header;
            boolean headerProcessed = false;
            Map<String, Integer> colIndexMap = new HashMap<>();

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] originalHeader = input.next();
                    header = Arrays.copyOf(originalHeader, originalHeader.length + 1);
                    header[header.length - 1] = newColumn;
                    for (int i = 0; i < originalHeader.length; i++) {
                        colIndexMap.put(originalHeader[i].trim(), i);
                    }
                    headerProcessed = true;
                    return header;
                }

                String[] row = input.next();
                String[] newRow = Arrays.copyOf(row, header.length);

                try {
                    double result = evaluateFormula(formula, row, colIndexMap);
                    newRow[newRow.length - 1] = String.valueOf(result);
                } catch (Exception e) {
                    newRow[newRow.length - 1] = ""; // default on error
                }

                return newRow;
            }
        };
    }

    private double evaluateFormula(String formula, String[] row, Map<String, Integer> colIndexMap) {
        String padded = formula.replace("+", " + ")
                               .replace("-", " - ")
                               .replace("*", " * ")
                               .replace("/", " / ");
        String[] tokens = padded.trim().split("\\s+");
        if (tokens.length == 0) return 0;

        double result = getValue(tokens[0], row, colIndexMap);

        for (int i = 1; i < tokens.length - 1; i += 2) {
            String op = tokens[i];
            double nextVal = getValue(tokens[i+1], row, colIndexMap);
            switch (op) {
                case "+" -> result += nextVal;
                case "-" -> result -= nextVal;
                case "*" -> result *= nextVal;
                case "/" -> result /= nextVal;
                default -> throw new RuntimeException("Unknown operator: " + op);
            }
        }
        return result;
    }

    private double getValue(String token, String[] row, Map<String, Integer> colIndexMap) {
        Integer idx = colIndexMap.get(token);
        if (idx != null && idx < row.length) {
            try { return Double.parseDouble(row[idx]); } catch (Exception e) { return 0; }
        }
        try { return Double.parseDouble(token); } catch (Exception e) { return 0; }
    }
}
