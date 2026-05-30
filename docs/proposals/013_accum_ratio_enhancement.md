# Change: enhance `accum_ratio`

Enhance `cyc/data_analysis.py:accum_ratio`:

1. Accept `pl.Expr` for `val1` and `val2`, not only column names.
2. Support one optional `filter` argument, applied before aggregation.
3. Sort row and column dimensions before pivoting. Dimensions produced by
   `cyc.cut` should use `sort_cut`; all other dimensions should use normal
   `sort`.

Implementation:

1. If `filter` is provided, filter the input dataframe first.
2. Build `__num__` and `__denom__` with `with_columns`, using the same value
   handling as other Polars expressions.
3. Aggregate before pivoting:
   `df.group_by(row + column).agg(pl.col("__num__").sum(), pl.col("__denom__").sum())`.
4. For every row or column dimension:
   - if it is a `cyc.cut` struct, call `sort_cut`;
   - otherwise collect it into the normal-sort columns.
5. Sort the remaining non-cut dimensions with `sort`.
6. Pivot the sorted grouped result for numerator and denominator, then compute
   ratios and margins as before.

Accepted tradeoff: normal sorting after `sort_cut` can become the primary order
when cut and non-cut dimensions are mixed. Keep the implementation simple and
do not add extra pivot-column ordering logic for that corner case.
