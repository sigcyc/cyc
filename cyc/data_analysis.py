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


def _value_expr(value: str | pl.Expr, filt: str | pl.Expr | None) -> pl.Expr:
    """Resolve a pivot value (column name or expression), masking it to 0 where `filt` is false."""
    expr = pl.col(value) if isinstance(value, str) else value
    if filt is not None:
        expr = pl.when(filt).then(expr).otherwise(0)
    return expr


def _is_cut(dtype: pl.DataType) -> bool:
    """A cyc.cut column is a struct carrying both breakpoint and category."""
    return isinstance(dtype, pl.Struct) and {f.name for f in dtype.fields} >= {"breakpoint", "category"}


def _sort_grouped(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    normal_columns = []
    for name in columns:
        if _is_cut(df.schema[name]):
            df = df.sort_cut(name)
        else:
            normal_columns.append(name)
    return df.sort(normal_columns, maintain_order=True) if normal_columns else df


def accum_ratio(
    df: pl.DataFrame,
    row: str | list[str],
    column: str | list[str],
    val1: str | pl.Expr,
    val2: str | pl.Expr,
    filt1: str | pl.Expr | None = None,
    filt2: str | pl.Expr | None = None,
) -> pl.DataFrame:
    if filt2 is None:
        filt2 = filt1

    df = df.with_columns(
        __num__=_value_expr(val1, filt1),
        __denom__=_value_expr(val2, filt2),
    )

    idx_cols = [row] if isinstance(row, str) else row
    column_cols = [column] if isinstance(column, str) else column
    grouped = df.group_by(idx_cols + column_cols).agg(
        pl.col("__num__").sum(),
        pl.col("__denom__").sum(),
    )
    grouped = _sort_grouped(grouped, idx_cols + column_cols)

    pv_num = grouped.pivot(on=column, index=row, values="__num__", aggregate_function="sum")
    pv_denom = grouped.pivot(on=column, index=row, values="__denom__", aggregate_function="sum")
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
