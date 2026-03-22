package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class ScaleStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String column = params.get("column");

        if (column == null) {
            throw new RuntimeException("Missing 'column' param for scale");
        }

        return new DataIterator() {

            boolean headerProcessed = false;
            String[] header;
            List<String[]> rows;
            double mean = 0;
            double stdDev = 0;
            int colIndex = -1;
            int index = 0;

            @Override
            public boolean hasNext() {
                if (!headerProcessed) return input.hasNext();
                if (rows == null) fetchAllAndCompute();
                return index < rows.size();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    header = input.next();
                    headerProcessed = true;
                    return header;
                }
                if (rows == null) fetchAllAndCompute();
                
                if (index < rows.size()) {
                    String[] row = rows.get(index++);
                    if (colIndex != -1 && colIndex < row.length) {
                        try {
                            double val = Double.parseDouble(row[colIndex]);
                            double scaled = stdDev == 0 ? 0 : (val - mean) / stdDev;
                            row[colIndex] = String.valueOf(scaled);
                        } catch (Exception ignored) {}
                    }
                    return row;
                }
                throw new RuntimeException("No more elements");
            }

            private void fetchAllAndCompute() {
                rows = new ArrayList<>();
                for (int i = 0; i < header.length; i++) {
                    if (header[i].trim().equals(column)) {
                        colIndex = i; break;
                    }
                }

                int count = 0;
                double sum = 0;
                
                while (input.hasNext()) {
                    String[] row = input.next().clone();
                    rows.add(row);
                    if (colIndex != -1 && colIndex < row.length) {
                        try {
                            double val = Double.parseDouble(row[colIndex]);
                            sum += val;
                            count++;
                        } catch (Exception ignored) {}
                    }
                }
                
                if (count > 0) {
                    mean = sum / count;
                    double varianceSum = 0;
                    for (String[] row : rows) {
                        if (colIndex != -1 && colIndex < row.length) {
                            try {
                                double val = Double.parseDouble(row[colIndex]);
                                varianceSum += Math.pow(val - mean, 2);
                            } catch (Exception ignored) {}
                        }
                    }
                    stdDev = Math.sqrt(varianceSum / count);
                }
            }
        };
    }
}
