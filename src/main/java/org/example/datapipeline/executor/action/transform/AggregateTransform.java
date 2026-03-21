package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.executor.context.ExecutionContext;

import java.util.*;

/**
 * Performs aggregation on dataset rows based on a grouping column.
 *
 * Groups data using a specified column and applies an aggregation
 * operation on another numeric column.
 *
 * Supported operations:
 * - sum   : total of all values in the group
 * - avg   : average of values in the group
 * - min   : minimum value in the group
 * - max   : maximum value in the group
 * - count : number of valid values in the group
 *
 * The method expects parameters for:
 * - group_by : column used to form groups
 * - column   : column on which aggregation is applied
 * - operation: aggregation type to perform
 *
 * Input data is read from the execution context and processed in memory.
 * The result is a reduced dataset with one row per group, containing
 * the group key and the computed aggregate value.
 *
 * Non-numeric values in the target column are ignored during aggregation.
 */
public class AggregateTransform implements TransformMethod {

    @Override
    public void apply(ExecutionContext ctx) {

        List<String[]> data = ctx.getData();
        Map<String, String> params = ctx.getMethod().getParamMap();

        if (data == null || data.isEmpty()) {
            throw new RuntimeException("No data available for aggregate");
        }

        String groupBy = params.get("group_by");
        String operation = params.get("operation");
        String column = params.get("column");

        if (groupBy == null || operation == null || column == null) {
            throw new RuntimeException("Missing params for aggregate");
        }

        String[] header = data.get(0);

        int groupIndex = -1, valueIndex = -1;

        for (int i = 0; i < header.length; i++) {
            if (header[i].equals(groupBy)) groupIndex = i;
            if (header[i].equals(column)) valueIndex = i;
        }

        if (groupIndex == -1 || valueIndex == -1) {
            throw new RuntimeException("Invalid columns for aggregation");
        }

        Map<String, List<Double>> groups = new HashMap<>();

        for (int i = 1; i < data.size(); i++) {

            String[] row = data.get(i);
            String key = row[groupIndex];

            try {
                double val = Double.parseDouble(row[valueIndex]);
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(val);
            } catch (Exception ignored) {}
        }

        List<String[]> result = new ArrayList<>();
        result.add(new String[]{groupBy, operation + "_" + column});

        for (Map.Entry<String, List<Double>> entry : groups.entrySet()) {

            List<Double> values = entry.getValue();

            double agg;

            switch (operation) {
                case "sum" -> agg = values.stream().mapToDouble(Double::doubleValue).sum();
                case "avg" -> agg = values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                case "min" -> agg = values.stream().mapToDouble(Double::doubleValue).min().orElse(0);
                case "max" -> agg = values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
                case "count" -> agg = values.size();
                default -> throw new RuntimeException("Invalid aggregation: " + operation);
            }

            result.add(new String[]{entry.getKey(), String.valueOf(agg)});
        }

        ctx.setData(result);
    }
}