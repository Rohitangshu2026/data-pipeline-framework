package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

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
