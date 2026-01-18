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

    row_sum = pl.sum_horizontal(val_cols)
    grand_total = pv.select(row_sum.sum())[0, 0]
    col_sum = pv.select(val_cols).sum()

    if norm_by == "R":
        pv = pv.with_columns(pl.col(c) / row_sum * 100 for c in val_cols)
    elif norm_by == "C":
        pv = pv.with_columns(pl.col(c) / col_sum[c][0] * 100 for c in val_cols)
    else:
        pv = pv.with_columns(pl.col(c) / grand_total * 100 for c in val_cols)

    pv = pv.with_columns(
        (row_sum / grand_total * 100).alias("row_pct"),
        row_sum.alias("row_sum"),
    )

    footer = pl.DataFrame(
        [
            {
                **{c: "col_pct" for c in idx_cols},
                **{c: col_sum[c][0] / grand_total * 100 for c in val_cols},
                "row_pct": 100.0,
                "row_sum": None,
            },
            {
                **{c: "col_sum" for c in idx_cols},
                **{c: col_sum[c][0] for c in val_cols},
                "row_pct": None,
                "row_sum": grand_total,
            },
        ]
    )

    return pl.concat([pv.with_columns(pl.col(c).cast(pl.String) for c in idx_cols), footer], how="vertical_relaxed")


def accum_ratio(
    df: pl.DataFrame,
    row: str | list[str],
    column: str | list[str],
    val1: str,
    val2: str,
    filt1: str | pl.Expr | None = None,
    filt2: str | pl.Expr | None = None,
) -> pl.DataFrame:
    if filt1 is not None:
        df = df.with_columns(pl.when(filt1).then(val1).otherwise(0))
    if filt2 is not None:
        df = df.with_columns(pl.when(filt2).then(val2).otherwise(0))
    pv_num = df.pivot(on=column, index=row, values=val1, aggregate_function="sum")
    pv_denom = df.pivot(on=column, index=row, values=val2, aggregate_function="sum")

    idx_cols = df.select(row).columns
    val_cols = [c for c in pv_num.columns if c not in idx_cols]

    # Cell ratios
    pv = pv_num.select(idx_cols).with_columns((pv_num[c] / pv_denom[c]).alias(c) for c in val_cols)

    # Row marginals
    row_sum_num = pl.sum_horizontal(pv_num.select(val_cols))
    row_sum_denom = pl.sum_horizontal(pv_denom.select(val_cols))
    pv = pv.with_columns(
        (row_sum_num / row_sum_denom).alias("row_ratio"),
        row_sum_denom.alias("row_sum"),
    )

    # Column marginals
    col_sum_num = pv_num.select(val_cols).sum()
    col_sum_denom = pv_denom.select(val_cols).sum()
    grand_num = col_sum_num.sum_horizontal()[0]
    grand_denom = col_sum_denom.sum_horizontal()[0]
    footer = pl.DataFrame(
        [
            {
                **{c: "col_ratio" for c in idx_cols},
                **{c: col_sum_num[c][0] / col_sum_denom[c][0] for c in val_cols},
                "row_ratio": grand_num / grand_denom,
                "row_sum": None,
            },
            {
                **{c: "col_sum" for c in idx_cols},
                **{c: col_sum_denom[c][0] for c in val_cols},
                "row_ratio": None,
                "row_sum": grand_denom,
            },
        ]
    )

    return pl.concat([pv.with_columns(pl.col(c).cast(pl.String) for c in idx_cols), footer], how="vertical_relaxed")
