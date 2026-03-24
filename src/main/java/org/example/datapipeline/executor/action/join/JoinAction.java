package org.example.datapipeline.executor.action.join;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.action.ActionExecutor;

import java.util.*;

/**
 * JoinAction implements an inner join operator for the data pipeline execution engine.
 *
 * This class supports two join strategies:
 *
 * 1. Hash Join (in-memory)
 *    - Used when the right dataset is small enough to fit into memory.
 *    - The right dataset is fully loaded into a hash map keyed by the join column.
 *    - The left dataset is streamed, and matching rows are probed against the hash map.
 *    - Produces output lazily using a streaming iterator.
 *
 * 2. Sort-Merge Join (external, disk-based)
 *    - Used when the right dataset is too large to fit in memory.
 *    - Both left and right datasets are externally sorted on the join key:
 *        • Data is read in chunks
 *        • Each chunk is sorted and written to temporary files
 *        • A k-way merge is performed using a priority queue
 *    - The sorted streams are then merged using a two-pointer technique:
 *        • Matching key groups are collected
 *        • A cross product is performed for matching groups
 *    - This approach avoids out-of-memory issues and supports large datasets.
 *
 * Execution Flow:
 * - The join type is validated (currently only "inner" is supported).
 * - Join keys and right source path are extracted from the execution context.
 * - The size of the right dataset is estimated.
 * - Based on a threshold, the system selects:
 *      • Hash Join if data fits in memory
 *      • Sort-Merge Join otherwise
 *
 * Output:
 * - The result is exposed as a streaming DataIterator.
 * - The header row is constructed by combining:
 *      • All columns from the left dataset
 *      • All non-key columns from the right dataset (prefixed with "right_")
 *
 * Key Components:
 *
 * - executeHashJoin:
 *   Builds an in-memory hash index on the right dataset and streams the left dataset.
 *
 * - executeSortMergeJoin:
 *   Performs external sorting on both datasets and merges them efficiently.
 *
 * - MergeIterator:
 *   Performs k-way merge over multiple sorted runs using a priority queue.
 *
 * - SortMergeIterator:
 *   Implements the merge phase of sort-merge join using a two-pointer approach
 *   with grouping for duplicate keys.
 *
 * - externalSort / externalSortFromIterator:
 *   Handles chunking, sorting, and spilling data to disk for large datasets.
 *
 * - findIndex:
 *   Utility method to locate the index of a join key in a header row.
 *
 * Assumptions and Limitations:
 * - Only inner joins are supported.
 * - Data is assumed to be CSV and already tokenized into String arrays.
 * - Join key comparison is lexicographic (String-based).
 * - Temporary files are written to disk and should be managed/cleaned externally.
 *
 * This implementation mirrors real-world database execution strategies and is designed
 * to balance memory usage and performance across different data sizes.
 */
public class JoinAction implements ActionExecutor {

    private int estimateSize(String path) {
        int count = 0;
        DataIterator it = new CsvDataIterator(path);
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
        String rightSrc = params.get("right_src");

        if (leftKey == null || rightKey == null || rightSrc == null) {
            throw new RuntimeException("Missing params for join (left_key, right_key, right_src required)");
        }

        int rightSize = estimateSize(rightSrc);
        // left size is streaming → assume large or estimate if needed

        final int MAX_BUILD_SIZE = 100_000;

        if (rightSize <= MAX_BUILD_SIZE) {
            executeHashJoin(ctx, leftKey, rightKey, rightSrc);
        } else {
            executeSortMergeJoin(ctx, leftKey, rightKey, rightSrc);
        }

    }

    private void executeHashJoin(ExecutionContext ctx, String leftKey, String rightKey, String rightSrc){
        // Load the right dataset into memory
        Map<String, List<String[]>> rightData = new HashMap<>();
        String[] rightHeader;

        DataIterator rightIt = new CsvDataIterator(rightSrc);
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

        // Define streaming Join output
        DataIterator leftIt = ctx.getIterator();

        DataIterator joinedIt = new DataIterator() {
            boolean headerProcessed = false;
            int leftKeyIdx = -1;

            Queue<String[]> buffer = new LinkedList<>();

            String[] nextRow = null;

            @Override
            public boolean hasNext() {
                if (!headerProcessed)
                    return true;
                if (nextRow != null)
                    return true;
                if (!buffer.isEmpty())
                    return true;

                while (leftIt.hasNext()) {
                    String[] leftRow = leftIt.next();
                    if (leftKeyIdx < leftRow.length) {
                        String keyVal = leftRow[leftKeyIdx];
                        List<String[]> matches = rightData.get(keyVal);
                        if (matches != null) {
                            for (String[] rightRow : matches) {
                                String[] joined = new String[leftRow.length + rightRow.length - 1];
                                System.arraycopy(leftRow, 0, joined, 0, leftRow.length);
                                int idx = leftRow.length;
                                for (int i = 0; i < rightRow.length; i++) {
                                    if (i != rightKeyIdx) {
                                        joined[idx++] = rightRow[i];
                                    }
                                }
                                buffer.add(joined);
                            }
                        }
                    }
                    if (!buffer.isEmpty()) {
                        nextRow = buffer.poll();
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] leftHeader = leftIt.next();
                    for (int i = 0; i < leftHeader.length; i++) {
                        if (leftHeader[i].trim().equalsIgnoreCase(leftKey)) {
                            leftKeyIdx = i;
                            break;
                        }
                    }
                    if (leftKeyIdx == -1) {
                        throw new RuntimeException("left_key not found in left dataset");
                    }

                    List<String> headerList = new ArrayList<>();
                    Collections.addAll(headerList, leftHeader);
                    for (int i = 0; i < rightHeader.length; i++) {
                        if (i != rightKeyIdx) {
                            headerList.add("right_" + rightHeader[i]);
                        }
                    }

                    String[] joinedHeader = headerList.toArray(new String[0]);
                    headerProcessed = true;
                    return joinedHeader;
                }

                if (nextRow != null) {
                    String[] result = nextRow;
                    nextRow = null;
                    return result;
                }

                if (!buffer.isEmpty()) {
                    return buffer.poll();
                }

                if (hasNext()) {
                    String[] result = nextRow;
                    nextRow = null;
                    return result;
                }

                throw new RuntimeException("No more elements");
            }
        };

        ctx.setIterator(joinedIt);
    }

    private void executeSortMergeJoin(ExecutionContext ctx, String leftKey, String rightKey, String rightSrc){
        try {
            // 1. get left key index
            DataIterator leftIt = ctx.getIterator();
            String[] leftHeader = leftIt.next();

            int leftKeyIdx = findIndex(leftHeader, leftKey);

            // 2. external sort LEFT
            List<String> leftRuns = externalSortFromIterator(leftIt, leftKeyIdx);

            // 3. external sort RIGHT
            DataIterator rightIt = new CsvDataIterator(rightSrc);
            String[] rightHeader = rightIt.next();

            int rightKeyIdx = findIndex(rightHeader, rightKey);

            List<String> rightRuns = externalSort(rightSrc, rightKeyIdx);

            // 4. merge iterators
            DataIterator sortedLeft = new MergeIterator(leftRuns, leftKeyIdx);
            DataIterator sortedRight = new MergeIterator(rightRuns, rightKeyIdx);

            // 5. now SAME merge logic as before (two pointer)

            ctx.setIterator(new SortMergeIterator(
                    sortedLeft, sortedRight,
                    leftHeader, rightHeader,
                    leftKeyIdx, rightKeyIdx
            ));

        } catch (Exception e) {
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
                    // MATCH → collect groups

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

                    // cross product
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

                List<String> header = new ArrayList<>();
                Collections.addAll(header, leftHeader);

                for (int i = 0; i < rightHeader.length; i++) {
                    if (i != rightKeyIdx) {
                        header.add("right_" + rightHeader[i]);
                    }
                }

                return header.toArray(new String[0]);
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

    private List<String> externalSort(String path, int keyIdx) throws Exception {
        final int CHUNK_SIZE = 50_000;

        List<String> tempFiles = new ArrayList<>();
        DataIterator it = new CsvDataIterator(path);

        String[] header = it.next(); // skip header

        while (it.hasNext()) {
            List<String[]> chunk = new ArrayList<>();

            for (int i = 0; i < CHUNK_SIZE && it.hasNext(); i++) {
                chunk.add(it.next());
            }

            // sort chunk
            chunk.sort(Comparator.comparing(row -> row[keyIdx]));

            // write to temp file
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
