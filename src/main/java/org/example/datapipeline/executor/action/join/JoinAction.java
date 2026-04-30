package org.example.datapipeline.executor.action.join;

import java.util.logging.Logger;
import java.io.*;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.action.ActionExecutor;

import java.util.*;

public class JoinAction implements ActionExecutor {

    private static final Logger logger = Logger.getLogger(JoinAction.class.getName());

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

    private static int compareKeys(String a, String b) {
        try {
            return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

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
