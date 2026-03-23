package org.example.datapipeline.executor.action.transform;

import java.util.*;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

public class AggregateStrategy implements TransformStrategy {

    private static final int CHUNK_SIZE = 1000;

    @Override
    public DataIterator apply(DataIterator input, Method method) {
        Map<String, String> params = method.getParamMap();

        final String groupBy = params.get("group_by");
        final String operation = params.get("operation");
        final String column = params.get("column");

        if (groupBy == null || operation == null || column == null) {
            throw new RuntimeException("Missing params for aggregate");
        }

        if (!input.hasNext()) {
            throw new RuntimeException("Empty input data");
        }

        String[] header = input.next();

        int groupIndex = -1;
        int valueIndex = -1;

        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equalsIgnoreCase(groupBy)) {
                groupIndex = i;
            }
            if (header[i].trim().equalsIgnoreCase(column)) {
                valueIndex = i;
            }
        }

        if (groupIndex == -1 || valueIndex == -1) {
            throw new RuntimeException("Invalid columns for aggregation");
        }

        //GLOBAL REDUCE MAP
        Map<String, AggregateState> global = new HashMap<>();

        //CHUNKED MAP + REDUCE
        while (input.hasNext()) {

            List<String[]> chunk = new ArrayList<>();

            for (int i = 0; i < CHUNK_SIZE && input.hasNext(); i++) {
                chunk.add(input.next());
            }

            Map<String, AggregateState> local =
                    aggregateChunk(chunk, groupIndex, valueIndex);

            mergeMaps(global, local);
        }

        Iterator<Map.Entry<String, AggregateState>> iterator =
                global.entrySet().iterator();

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

                return new String[]{
                        entry.getKey(),
                        String.valueOf(result)
                };
            }
        };
    }

    //MAP PHASE
    private Map<String, AggregateState> aggregateChunk(
            List<String[]> chunk,
            int groupIndex,
            int valueIndex
    ) {
        Map<String, AggregateState> groups = new HashMap<>();

        for (String[] row : chunk) {
            if (groupIndex < row.length && valueIndex < row.length) {

                String key = row[groupIndex];
                Double val = tryParse(row[valueIndex]);

                if (val != null) {
                    groups.computeIfAbsent(key, k -> new AggregateState())
                            .add(val);
                }
            }
        }

        return groups;
    }

    //REDUCE PHASE
    private void mergeMaps(
            Map<String, AggregateState> global,
            Map<String, AggregateState> local
    ) {
        for (Map.Entry<String, AggregateState> entry : local.entrySet()) {
            global.computeIfAbsent(entry.getKey(), k -> new AggregateState())
                    .merge(entry.getValue());
        }
    }

    private Double tryParse(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return null;
        }
    }

    // FIXED + MERGEABLE STATE
    static class AggregateState {
        double sum = 0.0;
        int count = 0;
        double min = Double.MAX_VALUE;
        double max = Double.NEGATIVE_INFINITY;

        void add(double val) {
            sum += val;
            count++;
            min = Math.min(min, val);
            max = Math.max(max, val);
        }

        // merge support (MapReduce core)
        void merge(AggregateState other) {
            this.sum += other.sum;
            this.count += other.count;
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
        }

        double compute(String operation) {
            return switch (operation.toLowerCase()) {
                case "sum" -> sum;
                case "avg" -> count == 0 ? 0.0 : sum / count;
                case "min" -> count == 0 ? 0.0 : min;
                case "max" -> count == 0 ? 0.0 : max;
                case "count" -> count;
                default -> throw new RuntimeException("Invalid aggregation: " + operation);
            };
        }
    }
}