package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that replaces null or blank values in a single column with a default.
 *
 * <p>Required method parameters:
 * <ul>
 *   <li>{@code column} – name of the column to check</li>
 *   <li>{@code value}  – the replacement string to use when the cell is null or blank</li>
 * </ul>
 *
 * <p>A cell is treated as null/blank if it is Java {@code null}, an empty string, or a
 * whitespace-only string. All other cells (including non-null empty-ish values that contain
 * only spaces) are replaced. Cells that already have content are passed through unchanged.
 *
 * <p>The returned iterator is <em>lazy</em>: each row is cloned and the target cell is
 * conditionally replaced during iteration. The header row is passed through unchanged.
 *
 * <p>Typical use case: filling missing brand names with {@code "UNKNOWN"} or missing
 * category codes with {@code "uncategorized"} before downstream aggregation.
 */
public class FillNullsStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String column = params.get("column");
        String value = params.get("value");

        if (column == null || value == null) {
            throw new RuntimeException("Missing params for fill_nulls");
        }

        return new DataIterator() {

            boolean headerProcessed = false;
            int colIndex = -1;

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] header = input.next();
                    headerProcessed = true;
                    for (int i = 0; i < header.length; i++) {
                        if (header[i].trim().equals(column)) {
                            colIndex = i; break;
                        }
                    }
                    return header;
                }
                
                String[] row = input.next().clone();
                if (colIndex != -1 && colIndex < row.length) {
                    if (row[colIndex] == null || row[colIndex].trim().isEmpty()) {
                        row[colIndex] = value;
                    }
                }
                return row;
            }
        };
    }
}
