package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that applies Z-score standardisation to a single numeric column.
 *
 * <p>Required method parameter:
 * <ul>
 *   <li>{@code column} – name of the column to standardise</li>
 * </ul>
 *
 * <p><b>Algorithm:</b> For each value {@code v} in the column, the z-score is:
 * <pre>    z = (v - mean) / stdDev</pre>
 * where {@code mean} is the arithmetic mean and {@code stdDev} is the population standard
 * deviation of all numeric values in that column. The result is dimensionless and centred
 * around 0: positive z-scores indicate above-average values, negative indicate below-average.
 * When the standard deviation is 0 (all values identical), the z-score is {@code 0.0}.
 *
 * <p><b>Two-pass implementation:</b>
 * <ol>
 *   <li><b>First pass</b> – all rows are buffered, and the sum and count of numeric values
 *       are accumulated to compute the mean.</li>
 *   <li><b>Second pass</b> – the buffered rows are scanned again to compute the sum of
 *       squared deviations (variance), from which the standard deviation is derived.</li>
 * </ol>
 *
 * <p>Memory usage is O(N) — all rows are stored in a {@code List<String[]>} before any
 * output is produced.
 *
 * <p>Non-numeric values in the target column are silently ignored in both passes and left
 * unchanged in the output.
 *
 * <p>Typical use case: converting average transaction prices into z-scores so that brands
 * with above-average prices score positively and below-average brands score negatively.
 */
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
