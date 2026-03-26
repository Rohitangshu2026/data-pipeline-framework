package org.example.datapipeline.executor.action.transform;

import java.util.*;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * AggregateStrategy implements a grouped aggregation operator for the data pipeline.
 *
 * This class performs aggregations such as SUM, AVG, MIN, MAX, and COUNT over
 * a dataset grouped by a specified column. It follows a chunked MapReduce-style
 * execution model to balance memory efficiency and scalability.
 *
 * Execution Model:
 *
 * 1. Map Phase (per chunk)
 *    - Input data is processed in fixed-size chunks to avoid loading the entire dataset into memory.
 *    - Each chunk is grouped by the specified "group_by" column.
 *    - For each group, an AggregateState object accumulates:
 *        • sum
 *        • count
 *        • min
 *        • max
 *
 * 2. Local Aggregation
 *    - Within each chunk, rows are aggregated into a local map:
 *        Map<groupKey, AggregateState>
 *
 * 3. Reduce Phase (global merge)
 *    - Local maps from each chunk are merged into a global aggregation map.
 *    - Aggregation states are combined using a mergeable structure:
 *        • sums are added
 *        • counts are added
 *        • min/max are updated
 *
 * 4. Final Output
 *    - Results are exposed as a streaming DataIterator.
 *    - Each row contains:
 *        • group key
 *        • computed aggregation value
 *    - Header format:
 *        [group_by, operation_column]
 *
 * Key Components:
 *
 * - apply:
 *   Entry point that validates parameters, identifies column indices,
 *   and orchestrates chunked aggregation and final output.
 *
 * - aggregateChunk:
 *   Performs the map phase on a single chunk of rows, producing a local aggregation map.
 *
 * - mergeMaps:
 *   Combines local aggregation maps into the global result using mergeable states.
 *
 * - AggregateState:
 *   A compact, mergeable structure that tracks sum, count, min, and max.
 *   Supports efficient combination across chunks and final computation.
 *
 * - tryParse:
 *   Safely parses numeric values, ignoring invalid or non-numeric inputs.
 *
 * Supported Operations:
 * - sum
 * - avg
 * - min
 * - max
 * - count
 *
 * Design Characteristics:
 * - Memory efficient via chunk-based processing
 * - Inspired by MapReduce execution (map → combine → reduce)
 * - Streaming output via iterator abstraction
 * - Robust to malformed numeric data (ignored during aggregation)
 *
 * Assumptions and Limitations:
 * - Input rows are String arrays representing CSV-like data.
 * - Aggregations operate only on numeric columns.
 * - Non-numeric values are skipped silently.
 * - Group keys are treated as strings and compared lexicographically.
 *
 * This implementation provides a scalable and extensible foundation for
 * aggregation operations in a data pipeline engine.
 */
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
            String key = (groupIndex < row.length) ? row[groupIndex] : "UNKNOWN";

            String valStr = (valueIndex < row.length) ? row[valueIndex] : null;
            Double val = tryParse(valStr);

            if (key == null || key.trim().isEmpty()) {
                key = "UNKNOWN";
            }
            AggregateState state = groups.computeIfAbsent(key, k -> new AggregateState());
            state.incrementRow();   // ALWAYS count row

            if (val != null) {
                state.add(val);
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
        int rowCount = 0;      // total rows
        int valueCount = 0;    // numeric values only
        double min = Double.MAX_VALUE;
        double max = Double.NEGATIVE_INFINITY;

        void add(Double val) {
            if (val != null) {
                sum += val;
                min = Math.min(min, val);
                max = Math.max(max, val);
                valueCount++;
            }
        }

        void incrementRow() {
            rowCount++;
        }

        void merge(AggregateState other) {
            this.sum += other.sum;
            this.rowCount += other.rowCount;
            this.valueCount += other.valueCount;
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
        }

        double compute(String operation) {
            return switch (operation.toLowerCase()) {
                case "sum" -> sum;
                case "avg" -> valueCount == 0 ? 0.0 : sum / valueCount;
                case "min" -> valueCount == 0 ? 0.0 : min;
                case "max" -> valueCount == 0 ? 0.0 : max;
                case "count" -> rowCount;   // 👈 correct semantics
                default -> throw new RuntimeException("Invalid aggregation: " + operation);
            };
        }
    }
}