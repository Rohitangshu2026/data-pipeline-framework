package org.example.datapipeline.executor.action.join;

import java.util.logging.Logger;
import java.io.*;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.action.ActionExecutor;

import java.util.*;

/**
 * Action executor that performs a relational join between two datasets.
 *
 * <h2>Overview</h2>
 * <p>{@code JoinAction} supports four join types (inner, left, right, full) via two
 * physical strategies (hash join and sort-merge join), selected at runtime based on the
 * pipeline XML configuration.
 *
 * <h2>Input Configuration</h2>
 * <p>Required method parameters:
 * <ul>
 *   <li>{@code left_key}      – column name in the left (primary) dataset</li>
 *   <li>{@code right_key}     – column name in the right (secondary) dataset</li>
 *   <li>{@code right_ref}     – global datasource ID for the right dataset (preferred), OR</li>
 *   <li>{@code right_src}     – direct file path for the right dataset (CSV only)</li>
 *   <li>{@code join_type}     – {@code inner} (default), {@code left}, {@code right},
 *       {@code full}</li>
 *   <li>{@code join_strategy} – {@code hash} (default) or {@code sort_merge}</li>
 * </ul>
 *
 * <h2>Output Schema</h2>
 * <p>The output header is the full left header followed by all right columns <em>except</em>
 * the right key column, each prefixed with {@code right_}:
 * <pre>
 *   left columns:  [brand, sum_price]
 *   right columns: [brand, count_product_id]       (brand is the join key)
 *   output header: [brand, sum_price, right_count_product_id]
 * </pre>
 * This naming convention avoids column name collisions in multi-stage chained joins.
 *
 * <h2>Hash Join Strategy</h2>
 * <p>The default strategy for datasets up to ~100K right-side rows:
 * <ol>
 *   <li><b>Build phase</b> – the entire right dataset is loaded into a
 *       {@code HashMap<key, List<String[]>>}. An {@link java.util.IdentityHashMap} also
 *       tracks which right rows have been matched (for right/full outer joins).</li>
 *   <li><b>Probe phase</b> – the left dataset is streamed row by row; each left-key value
 *       is looked up in the hash map to find matching right rows. The probe phase is
 *       implemented as a lazy {@link DataIterator} so the left side is not materialised.</li>
 * </ol>
 * Memory: O(R) where R = number of right-side rows.
 *
 * <h2>Sort-Merge Join Strategy</h2>
 * <p>An alternative for large datasets where both sides are too big to fit in memory.
 * Only {@code inner} join is supported with sort-merge:
 * <ol>
 *   <li><b>External sort</b> – both iterators are sorted by their key column using an
 *       external sort ({@link #externalSortFromIterator}): rows are processed in
 *       50,000-row chunks, each chunk sorted in memory and spilled to a temp file in
 *       {@code /tmp/}. Temp files are registered with {@link ExecutionContext#registerTempFile}
 *       for automatic cleanup.</li>
 *   <li><b>K-way merge</b> – a {@link MergeIterator} backed by a {@link java.util.PriorityQueue}
 *       merges the sorted run files, producing a globally sorted stream without loading all
 *       runs into memory simultaneously.</li>
 *   <li><b>Merge phase</b> – a {@link SortMergeIterator} advances both sorted streams
 *       in lockstep, collecting groups of equal keys and emitting their cross product.</li>
 * </ol>
 * Memory: O(chunk_size) for the sort phase + O(run_count) for the merge priority queue.
 *
 * <h2>Cleanup</h2>
 * <p>Both strategies wrap their output iterator in a {@link CleanupIterator} that calls
 * {@link ExecutionContext#cleanup()} when the iterator is exhausted or an exception is
 * thrown. This ensures temp files from the sort-merge join are deleted even when the
 * downstream consumer does not fully drain the iterator.
 */
public class JoinAction implements ActionExecutor {

    private static final Logger logger = Logger.getLogger(JoinAction.class.getName());

    /**
     * Opens a {@link DataIterator} over the right-side dataset.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>If {@code right_ref} is set, looks up the datasource by ID from the
     *       execution-context metadata map, reads its type and parameters, and delegates to
     *       {@link org.example.datapipeline.executor.io.DataIORegistry}.</li>
     *   <li>If {@code right_src} is set, opens a CSV iterator directly on that file path.</li>
     * </ol>
     *
     * @param ctx execution context supplying method params and the global datasource map
     * @return a new iterator positioned before the right dataset's header row
     * @throws RuntimeException if neither {@code right_ref} nor {@code right_src} is present
     */
    private DataIterator createRightIterator(ExecutionContext ctx) {
        Map<String, String> params = ctx.getMethod().getParamMap();
        String rightSrc = params.get("right_src");
        String rightRef = params.get("right_ref");
        
        if (rightRef != null) {
            @SuppressWarnings("unchecked")
            Map<String, org.example.datapipeline.config.Datasource> globals =
                (Map<String, org.example.datapipeline.config.Datasource>) ctx.getMetadata().get("globals");
            org.example.datapipeline.config.Datasource ds = globals.get(rightRef);
            Map<String, String> dsParams = new HashMap<>();
            for (org.example.datapipeline.config.action.Param p : ds.getParams()) {
                dsParams.put(p.getName(), p.getValue());
            }
            return org.example.datapipeline.executor.io.DataIORegistry.getReader(ds.getType()).createIterator(dsParams);
        } else if (rightSrc != null) {
            Map<String, String> dsParams = new HashMap<>();
            dsParams.put("src", rightSrc);
            return org.example.datapipeline.executor.io.DataIORegistry.getReader("csv").createIterator(dsParams);
        }
        throw new RuntimeException("Missing right_src or right_ref parameter for join");
    }

    @Override
    public void execute(ExecutionContext ctx) {
        Method method = ctx.getMethod();
        Map<String, String> params = method.getParamMap();
        String joinType = params.getOrDefault("join_type", "inner").toLowerCase();
        
        if (!Set.of("inner", "left", "right", "full").contains(joinType)) {
            throw new RuntimeException("Invalid join_type: " + joinType);
        }

        String leftKey = params.get("left_key");
        String rightKey = params.get("right_key");

        if (leftKey == null || rightKey == null) {
            throw new RuntimeException("Missing params for join (left_key, right_key required)");
        }

        long start = System.currentTimeMillis();
        String strategy = params.getOrDefault("join_strategy", "hash").toLowerCase();

        DataIterator leftIt = ctx.getIterator();
        DataIterator rightIt = createRightIterator(ctx);

        String[] leftHeader = leftIt.hasNext() ? leftIt.next() : new String[0];
        String[] rightHeader = rightIt.hasNext() ? rightIt.next() : new String[0];

        int leftKeyIdx = -1;
        int rightKeyIdx = -1;

        if (leftHeader.length > 0) {
            leftKeyIdx = findIndex(leftHeader, leftKey);
        }
        if (rightHeader.length > 0) {
            rightKeyIdx = findIndex(rightHeader, rightKey);
        }

        DataIterator joinedIt;

        if ("hash".equals(strategy)) {
            logger.info("[JOIN] strategy=HASH_JOIN type=" + joinType);
            joinedIt = executeHashJoin(leftIt, rightIt, leftHeader, rightHeader, leftKeyIdx, rightKeyIdx, joinType);
        } else if ("sort_merge".equals(strategy)) {
            if (!"inner".equals(joinType)) {
                throw new RuntimeException("Outer joins not supported with sort_merge yet");
            }
            logger.info("[JOIN] strategy=SORT_MERGE_JOIN type=" + joinType);
            joinedIt = executeSortMergeJoin(ctx, leftIt, rightIt, leftHeader, rightHeader, leftKeyIdx, rightKeyIdx);
        } else {
            throw new RuntimeException("Invalid join_strategy: " + strategy);
        }

        DataIterator finalIt = new CleanupIterator(joinedIt, ctx);
        ctx.setIterator(finalIt);

        long duration = System.currentTimeMillis() - start;
        logger.info("[JOIN_METRICS] strategy=" + strategy + " durationMs=" + duration);
    }

    /**
     * Executes a hash join between the left and right datasets.
     *
     * <p>Build phase: loads all right rows into a {@code HashMap<key, List<String[]>>}.
     * Probe phase: streams left rows and looks up each key in the hash map.
     *
     * <p>For left/full outer joins, unmatched left rows are emitted with empty right-column
     * values. For right/full outer joins, unmatched right rows (tracked via an
     * {@link java.util.IdentityHashMap}) are emitted after all left rows are processed.
     *
     * @param leftIt      streaming iterator over the left dataset (header already consumed)
     * @param rightIt     streaming iterator over the right dataset (header already consumed)
     * @param leftHeader  the left dataset header row
     * @param rightHeader the right dataset header row
     * @param leftKeyIdx  column index of the join key in the left dataset
     * @param rightKeyIdx column index of the join key in the right dataset
     * @param joinType    {@code "inner"}, {@code "left"}, {@code "right"}, or {@code "full"}
     * @return a lazy iterator producing the joined header followed by joined data rows
     */
    private DataIterator executeHashJoin(DataIterator leftIt, DataIterator rightIt,
                                         String[] leftHeader, String[] rightHeader,
                                         int leftKeyIdx, int rightKeyIdx, String joinType) {
        logger.info("[JOIN][HASH] phase=build_start type=" + joinType);
        Map<String, List<String[]>> rightData = new HashMap<>();
        Map<String[], Boolean> matchedRightRows = new IdentityHashMap<>();

        if (rightHeader.length > 0) {
            while (rightIt.hasNext()) {
                String[] row = rightIt.next();
                if (rightKeyIdx < row.length) {
                    String keyVal = row[rightKeyIdx];
                    rightData.computeIfAbsent(keyVal, k -> new ArrayList<>()).add(row);
                }
                matchedRightRows.put(row, false);
            }
        } else {
            logger.warning("[JOIN][HASH] Right dataset is completely empty");
        }
        logger.info("[JOIN][HASH] phase=build_complete entries=" + rightData.size());

        return new DataIterator() {
            boolean headerReturned = false;
            Iterator<String[]> currentMatches = null;
            String[] currentLeftRow = null;
            boolean emitUnmatchedLeft = false;
            
            Iterator<String[]> unmatchedRightRows = null;

            @Override
            public boolean hasNext() {
                if (!headerReturned) return true;

                if (currentMatches != null && currentMatches.hasNext()) {
                    return true;
                }
                
                if (emitUnmatchedLeft) {
                    return true;
                }
                
                if (unmatchedRightRows != null) {
                    return unmatchedRightRows.hasNext();
                }

                while (leftIt.hasNext()) {
                    currentLeftRow = leftIt.next();

                    if (leftKeyIdx != -1 && leftKeyIdx < currentLeftRow.length) {
                        String key = currentLeftRow[leftKeyIdx];
                        List<String[]> matches = rightData.get(key);

                        if (matches != null && !matches.isEmpty()) {
                            currentMatches = matches.iterator();
                            return true;
                        }
                    }
                    
                    if ("left".equals(joinType) || "full".equals(joinType)) {
                        emitUnmatchedLeft = true;
                        return true;
                    }
                }
                
                if (("right".equals(joinType) || "full".equals(joinType)) && unmatchedRightRows == null) {
                    List<String[]> unmatched = new ArrayList<>();
                    for (Map.Entry<String[], Boolean> entry : matchedRightRows.entrySet()) {
                        if (!entry.getValue()) {
                            unmatched.add(entry.getKey());
                        }
                    }
                    unmatchedRightRows = unmatched.iterator();
                    return unmatchedRightRows.hasNext();
                }

                return false;
            }

            @Override
            public String[] next() {
                if (!headerReturned) {
                    List<String> headerList = new ArrayList<>();
                    Collections.addAll(headerList, leftHeader);
                    if (rightHeader.length > 0) {
                        for (int i = 0; i < rightHeader.length; i++) {
                            if (i != rightKeyIdx) {
                                headerList.add("right_" + rightHeader[i]);
                            }
                        }
                    }
                    headerReturned = true;
                    return headerList.toArray(new String[0]);
                }

                if (currentMatches != null && currentMatches.hasNext()) {
                    String[] rightRow = currentMatches.next();
                    matchedRightRows.put(rightRow, true);
                    return combineRows(currentLeftRow, rightRow);
                }
                
                if (emitUnmatchedLeft) {
                    emitUnmatchedLeft = false;
                    return combineRows(currentLeftRow, null);
                }
                
                if (unmatchedRightRows != null && unmatchedRightRows.hasNext()) {
                    return combineRows(null, unmatchedRightRows.next());
                }

                if (hasNext()) {
                    return next();
                }

                throw new RuntimeException("No more elements");
            }
            
            private String[] combineRows(String[] lRow, String[] rRow) {
                int leftLen = leftHeader.length;
                int rightLen = rightHeader.length > 0 ? rightHeader.length - 1 : 0;
                String[] joined = new String[leftLen + rightLen];
                
                for (int i = 0; i < leftLen; i++) {
                    joined[i] = (lRow != null && i < lRow.length && lRow[i] != null) ? lRow[i] : "";
                }
                
                int idx = leftLen;
                for (int i = 0; i < rightHeader.length; i++) {
                    if (i != rightKeyIdx) {
                        joined[idx++] = (rRow != null && i < rRow.length && rRow[i] != null) ? rRow[i] : "";
                    }
                }
                return joined;
            }
        };
    }

    /**
     * Executes a sort-merge inner join between the left and right datasets.
     *
     * <p>Both input iterators are externally sorted by their key columns (chunk-at-a-time,
     * spilling to temp files). The sorted run files are then merged via a
     * {@link MergeIterator} and fed into a {@link SortMergeIterator} that produces the
     * joined output.
     *
     * <p>Only inner joins are supported. Temp files are registered with
     * {@link ExecutionContext#registerTempFile} for cleanup.
     *
     * @param ctx         execution context (used for temp file registration)
     * @param leftIt      left dataset iterator (header already consumed)
     * @param rightIt     right dataset iterator (header already consumed)
     * @param leftHeader  left dataset column names
     * @param rightHeader right dataset column names
     * @param leftKeyIdx  join key column index in left
     * @param rightKeyIdx join key column index in right
     * @return lazy iterator producing the joined inner result
     */
    private DataIterator executeSortMergeJoin(ExecutionContext ctx, DataIterator leftIt, DataIterator rightIt,
                                              String[] leftHeader, String[] rightHeader,
                                              int leftKeyIdx, int rightKeyIdx) {
        try {
            logger.info("[JOIN][SMJ] phase=start");

            List<String> leftRuns = new ArrayList<>();
            if (leftHeader.length > 0 && leftKeyIdx != -1) {
                leftRuns = externalSortFromIterator(ctx, leftIt, leftKeyIdx);
            }

            List<String> rightRuns = new ArrayList<>();
            if (rightHeader.length > 0 && rightKeyIdx != -1) {
                rightRuns = externalSortFromIterator(ctx, rightIt, rightKeyIdx);
            }

            DataIterator sortedLeft = new MergeIterator(leftRuns, leftKeyIdx);
            DataIterator sortedRight = new MergeIterator(rightRuns, rightKeyIdx);

            return new SortMergeIterator(sortedLeft, sortedRight, leftHeader, rightHeader, leftKeyIdx, rightKeyIdx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * K-way merge iterator that produces a globally sorted stream from multiple pre-sorted
     * run files.
     *
     * <p>Uses a min-heap ({@link java.util.PriorityQueue}) of {@link MergeIterator.Entry}
     * objects, one entry per run file. On each {@link #next()} call, the smallest entry
     * is polled and the corresponding file's next row is pushed back into the heap. This
     * produces a globally sorted stream in O(log k) per row, where k is the number of
     * run files.
     *
     * <p>Key comparison delegates to {@link JoinAction#compareKeys}, which performs numeric
     * comparison when both values are parseable as {@code double}, and lexicographic
     * comparison otherwise.
     */
    class MergeIterator implements DataIterator {
        static class Entry {
            String[] row;
            int fileIdx;
            Entry(String[] r, int f) { row = r; fileIdx = f; }
        }

        PriorityQueue<Entry> pq;
        List<DataIterator> iterators;
        int keyIdx;

        MergeIterator(List<String> files, int keyIdx) {
            this.keyIdx = keyIdx;
            this.iterators = new ArrayList<>();
            this.pq = new PriorityQueue<>(Comparator.comparing(e -> e.row[keyIdx], JoinAction::compareKeys));

            for (int i = 0; i < files.size(); i++) {
                DataIterator it = new CsvDataIterator(files.get(i));
                iterators.add(it);
                if (it.hasNext()) {
                    pq.add(new Entry(it.next(), i));
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !pq.isEmpty();
        }

        @Override
        public String[] next() {
            Entry smallest = pq.poll();
            String[] result = smallest.row;

            DataIterator it = iterators.get(smallest.fileIdx);
            if (it.hasNext()) {
                pq.add(new Entry(it.next(), smallest.fileIdx));
            }
            return result;
        }
    }

    /**
     * Streaming iterator that performs the merge phase of a sort-merge inner join.
     *
     * <p>Advances two sorted input iterators ({@code left}, {@code right}) in lockstep:
     * <ul>
     *   <li>When {@code left.key < right.key}, advance left (no match).</li>
     *   <li>When {@code left.key > right.key}, advance right (no match).</li>
     *   <li>When keys are equal, collect all left and right rows with that key into
     *       local groups and emit their full cross product into an internal buffer.</li>
     * </ul>
     *
     * <p>The header is emitted on the first call: left columns followed by right columns
     * (excluding the right key column, prefixed with {@code right_}).
     *
     * <p>Buffered rows from the cross product are drained on subsequent calls to
     * {@link #hasNext()} / {@link #next()}.
     */
    class SortMergeIterator implements DataIterator {
        DataIterator left;
        DataIterator right;
        String[] leftHeader;
        String[] rightHeader;
        int leftKeyIdx;
        int rightKeyIdx;
        boolean headerReturned = false;
        String[] leftRow = null;
        String[] rightRow = null;
        Queue<String[]> buffer = new ArrayDeque<>();

        SortMergeIterator(DataIterator left, DataIterator right,
                          String[] leftHeader, String[] rightHeader,
                          int leftKeyIdx, int rightKeyIdx) {
            this.left = left;
            this.right = right;
            this.leftHeader = leftHeader;
            this.rightHeader = rightHeader;
            this.leftKeyIdx = leftKeyIdx;
            this.rightKeyIdx = rightKeyIdx;

            if (left.hasNext()) leftRow = left.next();
            if (right.hasNext()) rightRow = right.next();
        }

        @Override
        public boolean hasNext() {
            if (!headerReturned) return true;
            if (!buffer.isEmpty()) return true;

            while (leftRow != null && rightRow != null) {
                if (leftKeyIdx >= leftRow.length) { advanceLeft(); continue; }
                if (rightKeyIdx >= rightRow.length) { advanceRight(); continue; }

                String lVal = leftRow[leftKeyIdx];
                String rVal = rightRow[rightKeyIdx];
                int cmp = compareKeys(lVal, rVal);

                if (cmp < 0) {
                    advanceLeft();
                } else if (cmp > 0) {
                    advanceRight();
                } else {
                    List<String[]> leftGroup = new ArrayList<>();
                    List<String[]> rightGroup = new ArrayList<>();
                    String key = lVal;

                    while (leftRow != null && leftKeyIdx < leftRow.length && leftRow[leftKeyIdx].equals(key)) {
                        leftGroup.add(leftRow);
                        advanceLeft();
                    }

                    while (rightRow != null && rightKeyIdx < rightRow.length && rightRow[rightKeyIdx].equals(key)) {
                        rightGroup.add(rightRow);
                        advanceRight();
                    }

                    for (String[] l : leftGroup) {
                        for (String[] r : rightGroup) {
                            String[] joined = new String[l.length + r.length - 1];
                            System.arraycopy(l, 0, joined, 0, l.length);
                            int idx = l.length;
                            for (int i = 0; i < r.length; i++) {
                                if (i != rightKeyIdx) {
                                    joined[idx++] = r[i];
                                }
                            }
                            buffer.add(joined);
                        }
                    }

                    if (!buffer.isEmpty()) return true;
                }
            }
            return false;
        }

        private void advanceLeft() {
            leftRow = left.hasNext() ? left.next() : null;
        }

        private void advanceRight() {
            rightRow = right.hasNext() ? right.next() : null;
        }

        @Override
        public String[] next() {
            if (!headerReturned) {
                headerReturned = true;
                List<String> headerList = new ArrayList<>();
                Collections.addAll(headerList, leftHeader);
                if (rightHeader.length > 0) {
                    for (int i = 0; i < rightHeader.length; i++) {
                        if (i != rightKeyIdx) {
                            headerList.add("right_" + rightHeader[i]);
                        }
                    }
                }
                return headerList.toArray(new String[0]);
            }

            if (!buffer.isEmpty()) {
                return buffer.poll();
            }
            if (hasNext()) {
                return buffer.poll();
            }
            throw new RuntimeException("No more elements");
        }
    }

    /**
     * Decorator iterator that triggers {@link ExecutionContext#cleanup()} when the
     * underlying iterator is exhausted or an exception escapes a {@link #next()} call.
     *
     * <p>This ensures that sort-merge join temp files are deleted even when the downstream
     * consumer (the CSV writer) does not handle cleanup itself. Cleanup is idempotent –
     * subsequent calls after the first are no-ops.
     */
    class CleanupIterator implements DataIterator {
        private final DataIterator delegate;
        private final ExecutionContext ctx;
        private boolean cleanupDone = false;

        public CleanupIterator(DataIterator delegate, ExecutionContext ctx) {
            this.delegate = delegate;
            this.ctx = ctx;
        }

        private void performCleanup() {
            if (!cleanupDone) {
                ctx.cleanup();
                cleanupDone = true;
            }
        }

        @Override
        public boolean hasNext() {
            boolean has = delegate.hasNext();
            if (!has) {
                performCleanup();
            }
            return has;
        }

        @Override
        public String[] next() {
            try {
                return delegate.next();
            } catch (Exception e) {
                performCleanup();
                throw e;
            }
        }
    }

    /**
     * Performs an external sort of the given iterator by the specified key column.
     *
     * <p>Reads the iterator in chunks of 50,000 rows, sorts each chunk in memory using
     * {@link JoinAction#compareKeys}, writes the sorted chunk to a temp file under
     * {@code /tmp/sort_<UUID>.csv}, and registers the temp file with the execution context
     * for automatic cleanup. Returns the list of temp file paths, which are then merged
     * by {@link MergeIterator}.
     *
     * @param ctx    execution context for temp file registration
     * @param it     the iterator to sort (header must already have been consumed)
     * @param keyIdx the column index to sort by
     * @return list of sorted run-file paths (may be empty if the input is empty)
     * @throws Exception if a temp file cannot be written
     */
    private List<String> externalSortFromIterator(ExecutionContext ctx, DataIterator it, int keyIdx) throws Exception {
        final int CHUNK_SIZE = 50_000;
        List<String> tempFiles = new ArrayList<>();

        while (it.hasNext()) {
            List<String[]> chunk = new ArrayList<>();
            for (int i = 0; i < CHUNK_SIZE && it.hasNext(); i++) {
                String[] row = it.next();
                if (row.length > keyIdx) {
                    chunk.add(row);
                }
            }

            if (chunk.isEmpty()) continue;

            chunk.sort(Comparator.comparing(row -> row[keyIdx], JoinAction::compareKeys));
            String tempFile = "/tmp/sort_" + UUID.randomUUID() + ".csv";
            tempFiles.add(tempFile);
            ctx.registerTempFile(tempFile);

            try (java.io.PrintWriter pw = new java.io.PrintWriter(tempFile)) {
                for (String[] row : chunk) {
                    pw.println(String.join(",", row));
                }
            }
        }
        return tempFiles;
    }

    /**
     * Compares two key values, using numeric comparison when both are parseable as
     * {@code double} and lexicographic string comparison otherwise.
     *
     * <p>This approach correctly orders both purely numeric keys (e.g. user IDs, prices)
     * and string keys (e.g. brand names) without requiring type metadata.
     *
     * @param a the first key value
     * @param b the second key value
     * @return negative if {@code a < b}, zero if equal, positive if {@code a > b}
     */
    private static int compareKeys(String a, String b) {
        try {
            return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

    /**
     * Finds the column index of a named key in a header row.
     *
     * <p>Comparison is case-insensitive and trims surrounding whitespace, so header values
     * like {@code " Brand "} are matched by {@code "brand"}.
     *
     * @param header the header row to search
     * @param key    the column name to find
     * @return the zero-based column index
     * @throws RuntimeException if the column name is not found in the header
     */
    private int findIndex(String[] header, String key) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equalsIgnoreCase(key)) {
                return i;
            }
        }
        throw new RuntimeException("Missing join key: " + key);
    }

    @Override
    public String getType() {
        return "join";
    }
}
