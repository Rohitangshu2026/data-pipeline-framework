package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that sorts all data rows by a single column, ascending or descending.
 *
 * <p>Required method parameters:
 * <ul>
 *   <li>{@code column} – name of the column to sort by</li>
 *   <li>{@code order}  – sort direction: {@code "asc"} for ascending,
 *       {@code "desc"} for descending (case-insensitive)</li>
 * </ul>
 *
 * <p><b>Sort key type:</b> values are first attempted to be parsed as {@code double}; if
 * parsing succeeds for both rows, numeric comparison is used ({@link Double#compare}).
 * If either value is non-numeric, lexicographic string comparison is used.
 *
 * <p><b>Memory usage:</b> all data rows are materialised into a {@code List<String[]>}
 * before any output is produced (the sort cannot be lazy). The in-memory sort uses
 * Java's {@link java.util.List#sort} (TimSort), which is O(N log N) in time and O(N)
 * in space.
 *
 * <p>The header row is always emitted first and is not included in the sort.
 *
 * <p><b>Note:</b> a comment in the source mentions "External sorting can be added later".
 * For datasets that exceed available heap, an external merge-sort (similar to the
 * sort-merge join's {@code externalSortFromIterator}) would be the appropriate upgrade.
 *
 * <p>Typical use case: sorting brands by normalised revenue score (descending) before
 * applying a {@link LimitStrategy} to produce a top-N leaderboard.
 */
public class SortStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String column = params.get("column");
        String order = params.get("order");

        if (column == null || order == null) {
            throw new RuntimeException("Missing params for sort");
        }
        
        boolean asc = order.equalsIgnoreCase("asc");

        return new DataIterator() {

            boolean headerProcessed = false;
            String[] header;
            List<String[]> rows;
            int index = 0;

            @Override
            public boolean hasNext() {
                if (!headerProcessed) return input.hasNext();
                if (rows == null) {
                    fetchAllAndSort();
                }
                return index < rows.size();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    header = input.next();
                    headerProcessed = true;
                    return header;
                }
                if (rows == null) {
                    fetchAllAndSort();
                }
                if (index < rows.size()) {
                    return rows.get(index++);
                }
                throw new RuntimeException("No more elements");
            }
            
            private void fetchAllAndSort() {
                // External sorting can be added later
                rows = new ArrayList<>();
                while (input.hasNext()) {
                    rows.add(input.next());
                }
                
                int colIndex = -1;
                for (int i = 0; i < header.length; i++) {
                    if (header[i].trim().equals(column)) {
                        colIndex = i; break;
                    }
                }
                final int ci = colIndex;
                
                if (ci != -1) {
                    rows.sort((r1, r2) -> {
                        String v1 = ci < r1.length ? r1[ci] : "";
                        String v2 = ci < r2.length ? r2[ci] : "";
                        try {
                            double d1 = Double.parseDouble(v1);
                            double d2 = Double.parseDouble(v2);
                            return asc ? Double.compare(d1, d2) : Double.compare(d2, d1);
                        } catch (NumberFormatException e) {
                            return asc ? v1.compareTo(v2) : v2.compareTo(v1);
                        }
                    });
                }
            }
        };
    }
}
