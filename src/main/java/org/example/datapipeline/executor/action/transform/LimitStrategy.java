package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class LimitStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String countStr = params.get("count");

        if (countStr == null) {
            throw new RuntimeException("Missing count param for limit");
        }
        
        int limit = Integer.parseInt(countStr);

        return new DataIterator() {

            boolean headerProcessed = false;
            int currentCount = 0;

            @Override
            public boolean hasNext() {
                if (!headerProcessed) return input.hasNext();
                return currentCount < limit && input.hasNext();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    headerProcessed = true;
                    return input.next();
                }
                if (currentCount < limit) {
                    currentCount++;
                    return input.next();
                }
                throw new RuntimeException("Limit reached");
            }
        };
    }
}
