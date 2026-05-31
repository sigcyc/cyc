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

## Result type: `AccumRatioResult`

`accum_ratio` returns an `AccumRatioResult` (a `@dataclass(repr=False)`) instead
of a bare `pl.DataFrame`.

Fields:

- `df: pl.DataFrame` — the pivot table (cell ratios plus row/column margins). The
  main attribute.
- `row: list[pl.Expr]`, `column: list[pl.Expr]` — per-dimension key accessors
  (see [`filter` and `cyc.cut` columns](#filter-and-cyccut-columns)).
- `row_keys: list[tuple]`, `column_keys: list[tuple]` — the typed group key for
  each data row / each value column.

Behaviour:

- `__repr__` renders `df` in full (`pl.Config(tbl_rows=-1, tbl_cols=-1)`); the
  table is the point of the object.
- `filter(row: int | None, column: int | None) -> pl.Expr` maps a cell of the
  pivot table back to a predicate over the *original* dataframe. `row`/`column`
  are positional indices into the pivot rows / value columns. `row=None` filters
  by column only (and symmetrically `column=None` by row only). It builds
  `accessor == key` for each selected dimension and combines them with
  `pl.all_horizontal`.

Why typed keys are stored separately: the returned `df` casts its row-label
columns to `String` so the string footer labels (`col_ratio`, `col_sum`) can be
concatenated. Rebuilding a predicate from those strings would compare, say, an
`Int64` source column against a `"5"` literal. `row_keys` / `column_keys` keep
the pre-cast, correctly-typed values so `filter` produces type-correct
predicates.

## `filter` and `cyc.cut` columns

A `cyc.cut` column is a struct `{breakpoint, category}`. `_sort_grouped` calls
`sort_cut`, which **unnests** the struct and replaces the dimension with its
`category` string before the pivot. So inside the pivot the key is the category
label (e.g. `"(0, 100]"`), but in the original dataframe that column is still a
struct.

A naive `pl.col(name) == key` therefore compares a struct against a string and
silently matches zero rows. Fix: `_key_col(df, name)` returns the accessor used
in the predicate:

- `pl.col(name).struct.field("category")` when `name` is a cut struct
  (`_is_cut`);
- `pl.col(name)` otherwise.

`AccumRatioResult.row` / `.column` store these accessor expressions (not bare
names), so `filter` stays a single `accessor == key` per dimension with no
per-call branching. The `filtered` bucket (`breakpoint = NaN`,
`category = "filtered"`) round-trips like any other category.

## Pivot column ordering guarantee

`filter`'s column lookup assumes `val_cols` (the pivot's value-column order)
aligns positionally with
`column_keys = grouped.select(column).unique(maintain_order=True).rows()`. This
holds:

1. `pivot` is called with the defaults `sort_columns=False` and
   `maintain_order=True`. With `sort_columns=False`, transposed columns are
   emitted in *order of discovery* — the order each `on`-combination is first
   encountered while scanning the frame.
2. `unique(maintain_order=True)` is by definition that same first-appearance
   order.
3. Both `val_cols` and `column_keys` are derived from the **same `grouped`
   frame**, so they only need to agree with each other. Whatever order
   `_sort_grouped` happens to produce is irrelevant — pivot and `unique` both
   read first-appearance order off the identical row sequence.

Empirically verified: 0 mismatches across ~10k randomized trials (single- and
multi-column `on`, post-sort, nulls, NaN).

Caveat: this is polars' documented default ordering, not a frozen API contract.
If a future polars changes discovery order, `filter`'s column mapping breaks
silently (a wrong predicate, not an error). Hardening options, in increasing
cost:

- a regression test pinning `val_cols == [str(k) for k in column_keys]` on a
  known frame, so an upgrade fails loudly;
- carry the key through the same pivot (`values=pl.struct(column)`,
  `aggregate_function="first"`, read each output column's key from its own
  cells), removing the ordering dependency entirely at the cost of an extra
  pivot.

Current choice: keep the ordering-based approach (correct and minimal); the lock
test is the recommended safeguard.
