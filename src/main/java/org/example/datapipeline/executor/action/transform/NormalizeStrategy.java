package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that applies min-max normalisation to a single numeric column.
 *
 * <p>Required method parameter:
 * <ul>
 *   <li>{@code column} – name of the column to normalise</li>
 * </ul>
 *
 * <p><b>Algorithm:</b> For each value {@code v} in the column, the normalised value is
 * computed as:
 * <pre>    normalised = (v - min) / (max - min)</pre>
 * where {@code min} and {@code max} are the global minimum and maximum of the column
 * across all data rows. The result is in the range {@code [0.0, 1.0]}, where the row with
 * the smallest value gets {@code 0.0} and the row with the largest gets {@code 1.0}. When
 * all values are equal (min == max), the normalised value is {@code 0.0}.
 *
 * <p><b>Memory usage:</b> This strategy requires a full materialisation pass. On the first
 * call to {@link DataIterator#hasNext()} or {@link DataIterator#next()} after the header is
 * consumed, all remaining rows are loaded into a {@code List<String[]>} while the min/max
 * are computed. The normalised values are then written back into the buffered rows during
 * the output phase. Memory is O(N) where N is the number of data rows.
 *
 * <p>Non-numeric values in the target column are silently ignored during min/max computation
 * and left unchanged in the output.
 *
 * <p>Typical use case: converting raw revenue sums into a {@code [0, 1]} score where the
 * top-revenue brand receives exactly {@code 1.0000}.
 */
public class NormalizeStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String column = params.get("column");

        if (column == null) {
            throw new RuntimeException("Missing 'column' param for normalize");
        }

        return new DataIterator() {

            boolean headerProcessed = false;
            String[] header;
            List<String[]> rows;
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
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
                            double normalized = (max - min) == 0 ? 0 : (val - min) / (max - min);
                            row[colIndex] = String.valueOf(normalized);
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

                while (input.hasNext()) {
                    String[] row = input.next().clone();
                    rows.add(row);
                    if (colIndex != -1 && colIndex < row.length) {
                        try {
                            double val = Double.parseDouble(row[colIndex]);
                            min = Math.min(min, val);
                            max = Math.max(max, val);
                        } catch (Exception ignored) {}
                    }
                }
            }
        };
    }
}
