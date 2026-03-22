package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.Map;

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
