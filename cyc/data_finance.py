from __future__ import annotations

import numpy as np
import polars as pl
from numba import njit

from .time_util import next_trading_day, previous_trading_day
from .data_loaders import load_data


@njit(cache=True)
def _ewm_sum(t, v, c):
    r, s = np.empty(len(v)), 0.
    for i in range(len(v)):
        if i: s *= np.exp((t[i-1] - t[i]) * c)
        r[i] = s = s + v[i]
    return r


@pl.api.register_expr_namespace("ewm")
class Ewm:
    def __init__(self, e): self._e = e

    def sum(self, times: pl.Expr, half_life_ns: int) -> pl.Expr:
        c = 0.6931471805599453 / half_life_ns
        return pl.struct([self._e, times]).map_batches(lambda s: pl.Series(
            _ewm_sum(s.struct[1].cast(pl.Int64).to_numpy(),
                     s.struct[0].to_numpy().astype(np.float64), c)))


def add_stock(self: pl.DataFrame, fields: str | list[str], sym: str = 'sym') -> pl.DataFrame:
    """
    Join stock_data_day fields onto self by (sym, date).

    Args:
        fields: column name or list of column names to fetch

    Returns:
        DataFrame with sym, date, and requested fields
    """
    stock_data = load_data("stock_data_day", self["date"].unique())
    field_list = [fields] if isinstance(fields, str) else fields
    stock_data = stock_data.select(
        pl.col("ticker").alias(sym), "date", *field_list
    )
    return self.join(stock_data, on=[sym, "date"], how="left")


def add_spot(self: pl.DataFrame, num_days: int, field: str = "close") -> pl.DataFrame:
    """
    Get spot price adjusted for dividends and splits.

    Args:
        num_days: 0 for current, positive for forward, negative for backward
        field: price field to adjust (default: close)

    Returns:
        DataFrame with sym, date, and adjusted field
    """
    sym, date = self["sym"], self["date"]
    result = _get_spot(sym, date, num_days, field)
    name = f"spot_d{num_days}" if num_days >= 0 else f"spot_dm{-num_days}"
    return self.with_columns(result.alias(name))


def _get_spot(
    sym: pl.Series, date: pl.Series, num_days: int, field: str
) -> pl.Series:
    """Recursively compute adjusted spot price."""
    df = pl.DataFrame({"sym": sym, "date": date})

    if num_days == 0:
        return add_stock(df, field)[field]

    if num_days > 0:
        next_day = next_trading_day(date)
        spot = _get_spot(sym, next_day, num_days - 1, field)
        next_df = pl.DataFrame({"sym": sym, "date": next_day})
        adj = add_stock(next_df, ["dividend", "split"])
        dividend = adj["dividend"].fill_null(0)
        split = adj["split"].fill_null(1)
        return spot * split + dividend

    prev_day = previous_trading_day(date)
    spot = _get_spot(sym, prev_day, num_days + 1, field)
    adj = add_stock(df, ["dividend", "split"])
    dividend = adj["dividend"].fill_null(0)
    split = adj["split"].fill_null(1)
    return (spot - dividend) / split


