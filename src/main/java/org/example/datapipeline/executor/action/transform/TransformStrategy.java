package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

/**
 * Strategy interface for all row-level data transformation operations.
 *
 * <p>Each concrete implementation encapsulates a single transform algorithm
 * (filter, map, aggregate, join, normalize, etc.) and exposes it through a
 * uniform {@link #apply} signature. This follows the <em>Strategy</em> pattern:
 * {@link TransformAction} acts as the context that holds a registry of strategies
 * keyed by the method name declared in the pipeline XML.
 *
 * <p>Implementations should return a <em>lazy</em> {@link DataIterator} wherever
 * possible. A lazy iterator wraps the upstream iterator and transforms each row
 * on-demand rather than materialising the entire dataset. The exceptions are
 * stateful operations – {@link SortStrategy}, {@link NormalizeStrategy}, and
 * {@link ScaleStrategy} – which must buffer all rows to compute a global property
 * (sort key, min/max, mean/stddev) before emitting output.
 *
 * <p><b>Header contract:</b> the first row yielded by the returned iterator must
 * always be the (possibly modified) header row. Downstream strategies and the
 * CSV writer rely on this invariant.
 *
 * <p><b>Thread safety:</b> strategy instances are shared singletons stored in
 * {@link TransformAction}. Their {@link #apply} methods must therefore be
 * stateless – all per-execution state must live in the returned anonymous
 * {@link DataIterator}, not in instance fields of the strategy.
 */
public interface TransformStrategy {

    /**
     * Applies this transformation to the given input iterator and returns a new
     * iterator that emits the transformed rows.
     *
     * <p>The returned iterator <em>must</em> yield the header row first, followed by
     * zero or more data rows. Implementations that add, remove, or rename columns
     * must reflect those changes in the header row.
     *
     * @param input  the upstream data iterator; the first row is the header
     * @param method the method configuration block from the pipeline XML, providing
     *               access to named parameters via {@link Method#getParamMap()}
     * @return a new {@link DataIterator} producing the transformed output rows
     * @throws RuntimeException if required parameters are missing or column names are invalid
     */
    DataIterator apply(DataIterator input, Method method);
}
