package org.example.datapipeline.executor.action.join;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.CsvDataIterator;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.action.ActionExecutor;

import java.util.*;

public class JoinAction implements ActionExecutor {

    @Override
    public void execute(ExecutionContext ctx) {
        Method method = ctx.getMethod();
        String type = method.getName().toLowerCase();
        
        if (!type.equals("inner")) {
            throw new RuntimeException("Join currently only supports 'inner' method");
        }

        Map<String, String> params = method.getParamMap();
        String leftKey = params.get("left_key");
        String rightKey = params.get("right_key");
        String rightSrc = params.get("right_src");

        if (leftKey == null || rightKey == null || rightSrc == null) {
            throw new RuntimeException("Missing params for join (left_key, right_key, right_src required)");
        }

        // Load the right dataset into memory
        Map<String, List<String[]>> rightData = new HashMap<>();
        String[] rightHeader;
        int rightKeyIdx = -1;
        
        DataIterator rightIt = new CsvDataIterator(rightSrc);
        if (!rightIt.hasNext()) {
            throw new RuntimeException("Right dataset is empty");
        }
        
        rightHeader = rightIt.next();
        for (int i = 0; i < rightHeader.length; i++) {
            if (rightHeader[i].trim().equals(rightKey)) {
                rightKeyIdx = i;
                break;
            }
        }
        if (rightKeyIdx == -1) {
            throw new RuntimeException("right_key not found in right dataset");
        }
        
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

            @Override
            public boolean hasNext() {
                if (!headerProcessed) return leftIt.hasNext();
                if (!buffer.isEmpty()) return true;
                
                while (leftIt.hasNext()) {
                    String[] leftRow = leftIt.next();
                    if (leftKeyIdx < leftRow.length) {
                        String keyVal = leftRow[leftKeyIdx];
                        List<String[]> matches = rightData.get(keyVal);
                        if (matches != null) {
                            for (String[] rightRow : matches) {
                                String[] joined = new String[leftRow.length + rightRow.length];
                                System.arraycopy(leftRow, 0, joined, 0, leftRow.length);
                                System.arraycopy(rightRow, 0, joined, leftRow.length, rightRow.length);
                                buffer.add(joined);
                            }
                        }
                    }
                    if (!buffer.isEmpty()) return true;
                }
                return false;
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] leftHeader = leftIt.next();
                    for (int i = 0; i < leftHeader.length; i++) {
                        if (leftHeader[i].trim().equals(leftKey)) {
                            leftKeyIdx = i;
                            break;
                        }
                    }
                    if (leftKeyIdx == -1) {
                        throw new RuntimeException("left_key not found in left dataset");
                    }
                    
                    String[] joinedHeader = new String[leftHeader.length + rightHeader.length];
                    System.arraycopy(leftHeader, 0, joinedHeader, 0, leftHeader.length);
                    // prefix right columns 
                    for (int i = 0; i < rightHeader.length; i++) {
                        joinedHeader[leftHeader.length + i] = "right_" + rightHeader[i];
                    }
                    headerProcessed = true;
                    return joinedHeader;
                }
                
                if (!buffer.isEmpty() || hasNext()) {
                    return buffer.poll();
                }
                throw new RuntimeException("No more elements");
            }
        };
        
        ctx.setIterator(joinedIt);
    }

    @Override
    public String getType() {
        return "join";
    }
}
