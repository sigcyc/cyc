from typing import Literal
import polars as pl
from polars.selectors import Selector


def accum_ratiop(
    df: pl.DataFrame,
    row: str | Selector | list[str],
    column: str | Selector | list[str],
    values: str,
    *,
    filter: str | pl.Expr | None = None,
    norm_by: Literal["R", "C"] | None = None
) -> pl.DataFrame:
    """Pivot with percentages and marginal totals."""
    if filter is not None:
        df = df.with_columns(pl.when(filter).then(values).otherwise(0))
    pv = df.pivot(on=column, index=row, values=values, aggregate_function="sum")

    idx_cols = df.select(row).columns
    val_cols = [c for c in pv.columns if c not in idx_cols]

    row_sums = pl.sum_horizontal(val_cols)
    grand_total = pv.select(row_sums.sum())[0, 0]
    col_sums = pv.select(val_cols).sum()

    if norm_by == "R":
        pv = pv.with_columns(pl.col(c) / row_sums * 100 for c in val_cols)
    elif norm_by == "C":
        pv = pv.with_columns(pl.col(c) / col_sums[c][0] * 100 for c in val_cols)
    else:
        pv = pv.with_columns(pl.col(c) / grand_total * 100 for c in val_cols)

    pv = pv.with_columns(
        (row_sums / grand_total * 100).alias("row_pct"),
        row_sums.alias("row_sums"),
    )

    footer = pl.DataFrame([
        {**{c: "col_pct" for c in idx_cols},
         **{c: col_sums[c][0] / grand_total * 100 for c in val_cols},
         "row_pct": 100.0, "row_sums": None},
        {**{c: "col_sum" for c in idx_cols},
         **{c: col_sums[c][0] for c in val_cols},
         "row_pct": None, "row_sums": grand_total},
    ])

    return pl.concat(
        [pv.with_columns(pl.col(c).cast(pl.String) for c in idx_cols), footer],
        how="vertical_relaxed"
    )
