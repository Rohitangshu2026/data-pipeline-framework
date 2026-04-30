package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that emits at most N data rows from the input, then stops.
 *
 * <p>Required method parameter:
 * <ul>
 *   <li>{@code count} – maximum number of data rows to emit (the header row is always
 *       emitted and does not count towards this limit)</li>
 * </ul>
 *
 * <p>Once {@code count} data rows have been yielded, {@link DataIterator#hasNext()} returns
 * {@code false} regardless of how many rows remain in the upstream iterator. Upstream
 * resources are not explicitly closed by this strategy — the framework's {@code cleanup()}
 * path handles that.
 *
 * <p>The returned iterator is <em>lazy</em>: it delegates to the upstream iterator and
 * maintains only a counter in memory, making it O(1) in space.
 *
 * <p>Typical use case: extracting a top-N list after sorting (e.g. {@code limit(25)} after
 * sorting by revenue score to produce the brand top-25 scorecard).
 */
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
