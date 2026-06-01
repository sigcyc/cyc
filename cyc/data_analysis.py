from dataclasses import dataclass
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
    row = df.select(row).columns
    column = df.select(column).columns
    pv = df.pivot(on=column, index=row, values=values, aggregate_function="sum")

    val_cols = [c for c in pv.columns if c not in row]

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
                **{c: "col_pct" for c in row},
                **{c: col_sum[c][0] / grand_total * 100 for c in val_cols},
                "row_pct": 100.0,
                "row_sum": None,
            },
            {
                **{c: "col_sum" for c in row},
                **{c: col_sum[c][0] for c in val_cols},
                "row_pct": None,
                "row_sum": grand_total,
            },
        ]
    )

    return pl.concat([pv.with_columns(pl.col(c).cast(pl.String) for c in row), footer], how="vertical_relaxed")

def _is_cut(dtype: pl.DataType) -> bool:
    """A cyc.cut column is a struct carrying both breakpoint and category."""
    return isinstance(dtype, pl.Struct) and {f.name for f in dtype.fields} >= {"breakpoint", "category"}


def _key_col(df: pl.DataFrame, name: str) -> pl.Expr:
    """Match a cyc.cut column on its category label (it pivots/sorts to category)."""
    col = pl.col(name)
    return col.struct.field("category") if _is_cut(df.schema[name]) else col


def _sort_grouped(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    normal_columns = []
    for name in columns:
        if _is_cut(df.schema[name]):
            df = df.sort_cut(name)
        else:
            normal_columns.append(name)
    return df.sort(normal_columns, maintain_order=True) if normal_columns else df


@dataclass(repr=False)
class AccumRatioResult:
    df: pl.DataFrame
    row: list[pl.Expr]
    column: list[pl.Expr]
    row_keys: list[tuple]
    column_keys: list[tuple]

    def __repr__(self) -> str:
        with pl.Config(tbl_rows=-1, tbl_cols=-1):
            return repr(self.df)

    def filter(self, df: pl.DataFrame,  row: int | None, column: int | None) -> pl.DataFrame:
        exprs = []
        if row is not None:
            exprs += [c == v for c, v in zip(self.row, self.row_keys[row])]
        if column is not None:
            exprs += [c == v for c, v in zip(self.column, self.column_keys[column])]
        return df.filter(pl.all_horizontal(exprs))

    def add_index(self) -> "AccumRatioResult":
        """Display copy: each value cell shows `value (row,column)` for filter()."""
        n, labels = len(self.row_keys), len(self.row)
        val_cols = self.df.columns[labels:labels + len(self.column_keys)]
        i = pl.Series("__i", list(range(n)) + [None] * (self.df.height - n), dtype=pl.Int64)
        self.df =  self.df.with_columns(__i=i).with_columns(
            pl.when(pl.col("__i").is_not_null())
            .then(pl.col(c).round(2).cast(pl.String).fill_null("") + pl.format(" ({},{})", "__i", pl.lit(j)))
            .otherwise(pl.col(c).cast(pl.String)).alias(c)
            for j, c in enumerate(val_cols)
        ).drop("__i")
        return self


def accum_ratio(
    df: pl.DataFrame,
    row: str | list[str],
    column: str | list[str],
    val1: str | pl.Expr,
    val2: str | pl.Expr,
    filter: str | pl.Expr | None = None,
) -> AccumRatioResult:
    if filter is not None:
        df = df.filter(filter)

    df = df.with_columns(__num__=val1, __denom__=val2)

    row = [row] if isinstance(row, str) else row
    column = [column] if isinstance(column, str) else column
    grouped = df.group_by(row + column).agg(
        pl.col("__num__").sum(),
        pl.col("__denom__").sum(),
    )
    grouped = _sort_grouped(grouped, row + column)

    pv_num = grouped.pivot(on=column, index=row, values="__num__", aggregate_function="sum")
    pv_denom = grouped.pivot(on=column, index=row, values="__denom__", aggregate_function="sum")
    val_cols = [c for c in pv_num.columns if c not in row]

    # Cell ratios
    pv = pv_num.select(row).with_columns((pv_num[c] / pv_denom[c]).alias(c) for c in val_cols)

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
                **{c: "col_ratio" for c in row},
                **{c: None if col_sum_denom[c][0] == 0 else col_sum_num[c][0] / col_sum_denom[c][0] for c in val_cols},
                "row_ratio": grand_num / grand_denom,
                "row_sum": None,
            },
            {
                **{c: "col_sum" for c in row},
                **{c: col_sum_denom[c][0] for c in val_cols},
                "row_ratio": None,
                "row_sum": grand_denom,
            },
        ]
    )

    result = pl.concat([pv.with_columns(pl.col(c).cast(pl.String) for c in row), footer], how="vertical_relaxed")
    return AccumRatioResult(
        result,
        [_key_col(df, c) for c in row],
        [_key_col(df, c) for c in column],
        pv_num.select(row).rows(),
        grouped.select(column).unique(maintain_order=True).rows(),
    )
