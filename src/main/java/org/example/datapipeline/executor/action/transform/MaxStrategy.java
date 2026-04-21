package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.config.action.Method;

public class MaxStrategy implements TransformStrategy {
    @Override
    public DataIterator apply(DataIterator input, Method method) {
        String column = method.getParamMap().get("column");
        if (column == null) throw new RuntimeException("Missing 'column' param for 'max' action");
        if (!input.hasNext()) throw new RuntimeException("Empty input for 'max' action");

        String[] header = input.next();
        int targetIdx = -1;
        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equalsIgnoreCase(column)) {
                targetIdx = i;
                break;
            }
        }

        if (targetIdx == -1) throw new RuntimeException("Column '" + column + "' not found in stream");

        double maxVal = Double.NEGATIVE_INFINITY;
        boolean hasRows = false;

        while (input.hasNext()) {
            String[] row = input.next();
            if (targetIdx < row.length) {
                try {
                    double val = Double.parseDouble(row[targetIdx]);
                    if (val > maxVal) maxVal = val;
                    hasRows = true;
                } catch (NumberFormatException ignored) {}
            }
        }

        final double finalMax = maxVal;
        final boolean finalHasRows = hasRows;

        return new DataIterator() {
            int state = 0;
            @Override
            public boolean hasNext() {
                return state < 2;
            }
            @Override
            public String[] next() {
                if (state == 0) {
                    state++;
                    return new String[] {"max_" + column};
                } else if (state == 1) {
                    state++;
                    return new String[] {finalHasRows ? String.valueOf(finalMax) : "NaN"};
                }
                return null;
            }
        };
    }
}
