from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable, Literal
import polars as pl
from polars.selectors import Selector

if TYPE_CHECKING:
    from polars._typing import IntoExpr

Keys = str | pl.Expr | Iterable[str | pl.Expr]  # column name(s) or expression(s)


def accum_ratiop(
    df: pl.DataFrame,
    row: str | Selector | list[str],
    column: str | Selector | list[str],
    values: str,
    *,
    f: str | pl.Expr | None = None,
    norm_by: Literal["R", "C"] | None = None
) -> pl.DataFrame:
    """Pivot with percentages and marginal totals."""
    if f is not None:
        df = df.with_columns(pl.when(f).then(values).otherwise(0))
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
                **dict.fromkeys(row, "col_pct"),
                **{c: col_sum[c][0] / grand_total * 100 for c in val_cols},
                "row_pct": 100.0,
                "row_sum": None,
            },
            {
                **dict.fromkeys(row, "col_sum"),
                **{c: col_sum[c][0] for c in val_cols},
                "row_pct": None,
                "row_sum": grand_total,
            },
        ]
    )

    return pl.concat([pv.with_columns(pl.col(c).cast(pl.String) for c in row), footer], how="vertical_relaxed")


def _with_margins(cells: pl.DataFrame) -> pl.DataFrame:
    """Append each row's total as a `row_sum` column and each column's total as a last row."""
    cells = cells.with_columns(row_sum=pl.sum_horizontal(pl.all()))
    return pl.concat([cells, cells.sum()])


@dataclass(repr=False)
class GroupByResult:
    df: pl.DataFrame
    row: list[pl.Expr]
    column: list[pl.Expr]
    row_keys: list[tuple]
    column_keys: list[tuple]

    def __repr__(self) -> str:
        with pl.Config(tbl_rows=-1, tbl_cols=-1):
            return repr(self.df)

    def filter(
        self, df: pl.DataFrame, row: int | Iterable[int] | None, column: int | Iterable[int] | None
    ) -> pl.DataFrame:
        """Keep rows in any of the given row group(s) AND any of the given column group(s).

        row/column may be a single index, an iterable of indices, or None (no constraint).
        """
        def any_of(cols, keys, idx):
            idx = [idx] if isinstance(idx, int) else idx
            # inner all_horizontal: a row is in group i iff every key column equals group i's values (AND)
            # outer any_horizontal: keep the row if it falls in at least one requested group (OR)
            return pl.any_horizontal([pl.all_horizontal([c == v for c, v in zip(cols, keys[i])]) for i in idx])

        conds = []
        if row is not None:
            conds.append(any_of(self.row, self.row_keys, row))
        if column is not None:
            conds.append(any_of(self.column, self.column_keys, column))
        return df.filter(pl.all_horizontal(conds))

    def add_index(self) -> GroupByResult:
        """Display copy: each value cell shows `value (row,column)` for filter()."""
        n_rows, n_row_columns = len(self.row_keys), len(self.row)
        val_cols = self.df.columns[n_row_columns:n_row_columns + len(self.column_keys)]
        idx = pl.int_range(pl.len())
        self.df = self.df.with_columns(
            pl.when(idx < n_rows)
            .then(pl.col(c).round(2).cast(pl.String).fill_null("") + pl.format(" ({},{})", idx, pl.lit(j)))
            .otherwise(pl.col(c).round(2).cast(pl.String)).alias(c)
            for j, c in enumerate(val_cols)
        )
        return self


def _exprs(keys: Keys) -> list[pl.Expr]:
    keys = [keys] if isinstance(keys, (str, pl.Expr)) else keys
    return [pl.col(k) if isinstance(k, str) else k for k in keys]


@dataclass
class GroupBy:
    """Row x column group-by; each statistic comes back as a pivot table with margins."""

    df: pl.DataFrame
    row: list[pl.Expr]
    column: list[pl.Expr]

    def ratio(self, numerator: IntoExpr, denominator: IntoExpr) -> GroupByResult:
        """sum(numerator) / sum(denominator) in every cell and margin; row_sum/col_sum total the denominator."""

        def combine(num: pl.DataFrame, den: pl.DataFrame) -> pl.DataFrame:
            ratio = (num / den).rename({"row_sum": "row_ratio"}).with_columns(den["row_sum"])
            return pl.concat([ratio, den.tail(1)], how="diagonal_relaxed")

        return self._table([numerator, denominator], combine, ["col_ratio", "col_sum"])

    def sum(self, value: IntoExpr) -> GroupByResult:
        """sum(value) in every cell, with row_sum/col_sum margins."""
        return self._table([value], lambda table: table, ["col_sum"])

    def len(self) -> GroupByResult:
        """Row count in every cell, with row_sum/col_sum margins."""
        return self.sum(1)

    def _table(
        self, values: list[IntoExpr], combine: Callable[..., pl.DataFrame], footer: list[str]
    ) -> GroupByResult:
        """Group once, pivot each value's sum with margins, then label the rows of `combine(*tables)`."""
        sums = [f"__{i}__" for i in range(len(values))]
        row = [key.meta.output_name() for key in self.row]
        column = [key.meta.output_name() for key in self.column]
        grouped = (
            self.df.with_columns(**dict(zip(sums, values)))
            .group_by(self.row + self.column)
            .agg(pl.col(sums).sum())
            .sort(row)
        )
        column_keys = grouped.select(column).unique().sort(column)
        pivots = [
            grouped.pivot(on=column, on_columns=column_keys, index=row, values=s, aggregate_function="sum")
            for s in sums
        ]
        labels = pl.concat([pivots[0].select(pl.col(row).cast(pl.String)), pl.DataFrame(dict.fromkeys(row, footer))])
        return GroupByResult(
            labels.hstack(combine(*(_with_margins(pivot.drop(row)) for pivot in pivots))),
            self.row,
            self.column,
            pivots[0].select(row).rows(),
            column_keys.rows(),
        )


def gb(df: pl.DataFrame, row: Keys, column: Keys = pl.lit("all")) -> GroupBy:
    """Group by row x column keys; .ratio/.sum/.len each return a pivot table with margins.

    Keys are column names or expressions, e.g. pl.col("x").cyc.cut([...]). The
    default column is one constant "all" column, which makes a one-way table.
    """
    return GroupBy(df, _exprs(row), _exprs(column))
