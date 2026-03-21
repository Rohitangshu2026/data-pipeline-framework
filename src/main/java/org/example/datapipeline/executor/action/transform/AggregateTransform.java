package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Performs grouped aggregation on streaming input data.
 *
 * Consumes rows from the input iterator, groups them by a specified column,
 * and applies an aggregation operation on another numeric column.
 *
 * Supported operations:
 * - sum   : total of values in each group
 * - avg   : average value per group
 * - min   : minimum value per group
 * - max   : maximum value per group
 * - count : number of valid numeric entries per group
 *
 * The method expects parameters for:
 * - group_by : column used to form groups
 * - column   : column on which aggregation is performed
 * - operation: aggregation type to apply
 *
 * The input is processed in a streaming manner, but aggregation requires
 * maintaining intermediate state for each group in memory. Non-numeric
 * values are ignored during computation.
 *
 * The result is exposed as a new iterator producing:
 * - a header row with group and aggregated column
 * - one row per group containing the computed value
 *
 * This transform enables scalable group-based computations while
 * preserving compatibility with the framework's iterator-based execution model.
 */
public class AggregateTransform implements TransformMethod {

    @Override
    public DataIterator apply(DataIterator input, ExecutionContext ctx) {

        Map<String, String> params = ctx.getMethod().getParamMap();

        String groupBy = params.get("group_by");
        String operation = params.get("operation");
        String column = params.get("column");

        if (groupBy == null || operation == null || column == null) {
            throw new RuntimeException("Missing params for aggregate");
        }

        if (!input.hasNext()) {
            throw new RuntimeException("Empty input data");
        }

        String[] header = input.next();

        int groupIndex = -1, valueIndex = -1;

        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equalsIgnoreCase(groupBy)) groupIndex = i;
            if (header[i].trim().equalsIgnoreCase(column)) valueIndex = i;
        }

        if (groupIndex == -1 || valueIndex == -1) {
            throw new RuntimeException("Invalid columns for aggregation");
        }
        Map<String, AggregateState> groups = new HashMap<>();
        while (input.hasNext()) {
            String[] row = input.next();
            if (groupIndex >= row.length || valueIndex >= row.length) continue;
            String key = row[groupIndex];
            Double val = tryParse(row[valueIndex]);
            if (val == null) continue;
            groups.computeIfAbsent(key, k -> new AggregateState())
                    .add(val);
        }

        Iterator<Map.Entry<String, AggregateState>> iterator = groups.entrySet().iterator();

        return new DataIterator() {

            boolean headerReturned = false;

            @Override
            public boolean hasNext() {
                return !headerReturned || iterator.hasNext();
            }

            @Override
            public String[] next() {

                if (!headerReturned) {
                    headerReturned = true;
                    return new String[]{groupBy, operation + "_" + column};
                }

                Map.Entry<String, AggregateState> entry = iterator.next();
                double result = entry.getValue().compute(operation);

                return new String[]{entry.getKey(), String.valueOf(result)};
            }
        };
    }

    static class AggregateState {
        double sum = 0;
        int count = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        void add(double val) {
            sum += val;
            count++;
            min = Math.min(min, val);
            max = Math.max(max, val);
        }

        double compute(String operation) {
            return switch (operation) {
                case "sum" -> sum;
                case "avg" -> count == 0 ? 0 : sum / count;
                case "min" -> count == 0 ? 0 : min;
                case "max" -> count == 0 ? 0 : max;
                case "count" -> count;
                default -> throw new RuntimeException("Invalid aggregation: " + operation);
            };
        }
    }

    private Double tryParse(String s) {
        try { return Double.parseDouble(s); }
        catch (Exception e) { return null; }
    }
}