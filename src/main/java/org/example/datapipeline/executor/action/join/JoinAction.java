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

    private int estimateSize(ExecutionContext ctx) {
        int count = 0;
        DataIterator it = createRightIterator(ctx);
        if (it.hasNext()) it.next(); // skip header

        while (it.hasNext() && count < 10000) { // cap for speed
            it.next();
            count++;
        }
        return count == 10000 ? Integer.MAX_VALUE : count;
    }

    @Override
    public void execute(ExecutionContext ctx) {

        Method method = ctx.getMethod();
        String type = method.getName().toLowerCase();
        
        if (!type.equalsIgnoreCase("inner")) {
            throw new RuntimeException("Join currently only supports 'inner' method");
        }

        Map<String, String> params = method.getParamMap();
        String leftKey = params.get("left_key");
        String rightKey = params.get("right_key");

        if (leftKey == null || rightKey == null) {
            throw new RuntimeException("Missing params for join (left_key, right_key required)");
        }

        int rightSize = estimateSize(ctx);
        final int MAX_BUILD_SIZE = 100_000;

        long start = System.currentTimeMillis();

        String strategy;
        DataIterator rightIt = createRightIterator(ctx);

        if (rightSize <= MAX_BUILD_SIZE) {
            strategy = "HASH_JOIN";
            logger.info("[JOIN] strategy=HASH_JOIN rightSize=" + rightSize);
            executeHashJoin(ctx, leftKey, rightKey, rightIt);
        } else {
            strategy = "SORT_MERGE_JOIN";
            logger.info("[JOIN] strategy=SORT_MERGE_JOIN rightSize=" + rightSize);
            executeSortMergeJoin(ctx, leftKey, rightKey, rightIt);
        }

        long duration = System.currentTimeMillis() - start;

        logger.info("[JOIN_METRICS] strategy=" + strategy +
                " durationMs=" + duration +
                " rightSizeEstimate=" + rightSize);

    }

    private void executeHashJoin(ExecutionContext ctx, String leftKey, String rightKey, DataIterator rightIt){
        long start = System.currentTimeMillis();
        logger.info("[JOIN][HASH] phase=build_start");

        Map<String, List<String[]>> rightData = new HashMap<>();
        String[] rightHeader;

        if (!rightIt.hasNext()) {
            throw new RuntimeException("Right dataset is empty");
        }

        rightHeader = rightIt.next();
        int tempRightKeyIdx = -1;


        for (int i = 0; i < rightHeader.length; i++) {
            if (rightHeader[i].trim().equalsIgnoreCase(rightKey)) {
                tempRightKeyIdx = i;
                break;
            }
        }

        if (tempRightKeyIdx == -1) {
            throw new RuntimeException("right_key not found in right dataset");
        }

        final int rightKeyIdx = tempRightKeyIdx;


        while (rightIt.hasNext()) {
            String[] row = rightIt.next();
            if (rightKeyIdx < row.length) {
                String keyVal = row[rightKeyIdx];
                rightData.computeIfAbsent(keyVal, k -> new ArrayList<>()).add(row);
            }
        }

        logger.info("[JOIN][HASH] phase=build_complete entries=" + rightData.size());

        DataIterator leftIt = ctx.getIterator();

        DataIterator joinedIt = new DataIterator() {

            boolean headerReturned = false;
            int leftKeyIdx = -1;

            Iterator<String[]> currentMatches = null;
            String[] currentLeftRow = null;

            @Override
            public boolean hasNext() {

                if (!headerReturned) return true;

                if (currentMatches != null && currentMatches.hasNext()) {
                    return true;
                }

                while (leftIt.hasNext()) {
                    currentLeftRow = leftIt.next();

                    if (leftKeyIdx < currentLeftRow.length) {
                        String key = currentLeftRow[leftKeyIdx];
                        List<String[]> matches = rightData.get(key);

                        if (matches != null && !matches.isEmpty()) {
                            currentMatches = matches.iterator();
                            return true;
                        }
                    }
                }

                return false;
            }

            @Override
            public String[] next() {

                if (!headerReturned) {
                    String[] leftHeader = leftIt.next();

                    for (int i = 0; i < leftHeader.length; i++) {
                        if (leftHeader[i].equalsIgnoreCase(leftKey)) {
                            leftKeyIdx = i;
                            break;
                        }
                    }
                    if (leftKeyIdx == -1) {
                        throw new RuntimeException("left_key not found in left dataset");
                    }

                    List<String> header = new ArrayList<>();
                    Collections.addAll(header, leftHeader);

                    for (int i = 0; i < rightHeader.length; i++) {
                        if (i != rightKeyIdx) {
                            header.add("right_" + rightHeader[i]);
                        }
                    }

                    headerReturned = true;
                    return header.toArray(new String[0]);
                }

                if (currentMatches != null && currentMatches.hasNext()) {
                    String[] rightRow = currentMatches.next();

                    String[] joined =
                            new String[currentLeftRow.length + rightRow.length - 1];

                    System.arraycopy(currentLeftRow, 0, joined, 0, currentLeftRow.length);

                    int idx = currentLeftRow.length;
                    for (int i = 0; i < rightRow.length; i++) {
                        if (i != rightKeyIdx) {
                            joined[idx++] = rightRow[i];
                        }
                    }

                    return joined;
                }

                while (leftIt.hasNext()) {
                    currentLeftRow = leftIt.next();

                    String key = currentLeftRow[leftKeyIdx];
                    List<String[]> matches = rightData.get(key);

                    if (matches != null && !matches.isEmpty()) {
                        currentMatches = matches.iterator();
                        return next(); 
                    }
                }

                throw new RuntimeException("No more elements");
            }
        };

        ctx.setIterator(joinedIt);
        long duration = System.currentTimeMillis() - start;
        logger.info("[JOIN][HASH] phase=complete durationMs=" + duration);
    }

    private void executeSortMergeJoin(ExecutionContext ctx, String leftKey, String rightKey, DataIterator rightIt){
        try {
            long totalStart = System.currentTimeMillis();

            logger.info("[JOIN][SMJ] phase=start");

            DataIterator leftIt = ctx.getIterator();
            String[] leftHeader = leftIt.next();

            int leftKeyIdx = findIndex(leftHeader, leftKey);

            long leftSortStart = System.currentTimeMillis();
            List<String> leftRuns = externalSortFromIterator(leftIt, leftKeyIdx);
            long leftSortTime = System.currentTimeMillis() - leftSortStart;
            logger.info("[JOIN][SMJ] phase=external_sort_left durationMs=" +
                    leftSortTime + " runs=" + leftRuns.size());


            String[] rightHeader = rightIt.next();

            int rightKeyIdx = findIndex(rightHeader, rightKey);

            long rightSortStart = System.currentTimeMillis();
            List<String> rightRuns = externalSortFromIterator(rightIt, rightKeyIdx);
            long rightSortTime = System.currentTimeMillis() - rightSortStart;

            logger.info("[JOIN][SMJ] phase=external_sort_right durationMs=" +
                    rightSortTime + " runs=" + rightRuns.size());

            DataIterator sortedLeft = new MergeIterator(leftRuns, leftKeyIdx);
            DataIterator sortedRight = new MergeIterator(rightRuns, rightKeyIdx);

            long totalEnd = System.currentTimeMillis();

            logger.info("[JOIN][SMJ] phase=setup_complete durationMs=" +
                    (totalEnd - totalStart));
            ctx.setIterator(new SortMergeIterator(
                    sortedLeft, sortedRight,
                    leftHeader, rightHeader,
                    leftKeyIdx, rightKeyIdx
            ));

        } catch (Exception e) {
            logger.severe("[JOIN][SMJ] error=" + e.getMessage());
            throw new RuntimeException(e);
        }
    }


    class MergeIterator implements DataIterator {

        static class Entry {
            String[] row;
            int fileIdx;

            Entry(String[] r, int f) {
                row = r;
                fileIdx = f;
            }
        }

        PriorityQueue<Entry> pq;
        List<DataIterator> iterators;
        int keyIdx;

        MergeIterator(List<String> files, int keyIdx) {
            this.keyIdx = keyIdx;
            this.iterators = new ArrayList<>();
            this.pq = new PriorityQueue<>(
                    Comparator.comparing(e -> e.row[keyIdx])
            );

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

        SortMergeIterator(DataIterator left,
                          DataIterator right,
                          String[] leftHeader,
                          String[] rightHeader,
                          int leftKeyIdx,
                          int rightKeyIdx) {
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

                String lVal = leftRow[leftKeyIdx];
                String rVal = rightRow[rightKeyIdx];

                int cmp = lVal.compareTo(rVal);

                if (cmp < 0) {
                    advanceLeft();
                } else if (cmp > 0) {
                    advanceRight();
                } else {

                    List<String[]> leftGroup = new ArrayList<>();
                    List<String[]> rightGroup = new ArrayList<>();

                    String key = lVal;

                    while (leftRow != null && leftRow[leftKeyIdx].equals(key)) {
                        leftGroup.add(leftRow);
                        advanceLeft();
                    }

                    while (rightRow != null && rightRow[rightKeyIdx].equals(key)) {
                        rightGroup.add(rightRow);
                        advanceRight();
                    }

                    for (String[] l : leftGroup) {
                        for (String[] r : rightGroup) {

                            String[] joined =
                                    new String[l.length + r.length - 1];

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

                for (int i = 0; i < rightHeader.length; i++) {
                    if (i != rightKeyIdx) {
                        headerList.add("right_" + rightHeader[i]);
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

    private List<String> externalSortFromIterator(DataIterator it, int keyIdx) throws Exception {
        final int CHUNK_SIZE = 50_000;

        List<String> tempFiles = new ArrayList<>();

        while (it.hasNext()) {
            List<String[]> chunk = new ArrayList<>();

            for (int i = 0; i < CHUNK_SIZE && it.hasNext(); i++) {
                chunk.add(it.next());
            }

            chunk.sort(Comparator.comparing(row -> row[keyIdx]));

            String tempFile = "/tmp/sort_" + UUID.randomUUID() + ".csv";
            tempFiles.add(tempFile);

            try (java.io.PrintWriter pw = new java.io.PrintWriter(tempFile)) {
                for (String[] row : chunk) {
                    pw.println(String.join(",", row));
                }
            }
        }

        return tempFiles;
    }

    private int findIndex(String[] header, String key) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].trim().equalsIgnoreCase(key)) {
                return i;
            }
        }
        throw new RuntimeException("Key not found: " + key);
    }

    @Override
    public String getType() {
        return "join";
    }
}
