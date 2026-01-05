from __future__ import annotations

import polars as pl

from cyc.data_loaders import load_data
from cyc.time_util import next_trading_day, previous_trading_day


def get_stock(self: pl.DataFrame, fields: str | list[str]) -> pl.DataFrame:
    """
    Join stock_data_day fields onto self by (sym, date).

    Args:
        fields: column name or list of column names to fetch

    Returns:
        DataFrame with sym, date, and requested fields
    """
    stock_data = load_data(self["date"].unique(), "stock_data_day").df
    field_list = [fields] if isinstance(fields, str) else fields
    stock_data = stock_data.select("sym", "date", *field_list)

    return self.join(stock_data, on=["sym", "date"], how="left")


def get_spot(self: pl.DataFrame, num_days: int, field: str = "close") -> pl.DataFrame:
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
        return get_stock(df, field)[field]

    if num_days > 0:
        next_day = next_trading_day(date)
        spot = _get_spot(sym, next_day, num_days - 1, field)
        next_df = pl.DataFrame({"sym": sym, "date": next_day})
        adj = get_stock(next_df, ["dividend", "split"])
        dividend = adj["dividend"].fill_null(0)
        split = adj["split"].fill_null(1)
        return spot * split + dividend

    prev_day = previous_trading_day(date)
    spot = _get_spot(sym, prev_day, num_days + 1, field)
    adj = get_stock(df, ["dividend", "split"])
    dividend = adj["dividend"].fill_null(0)
    split = adj["split"].fill_null(1)
    return (spot - dividend) / split


pl.DataFrame.get_stock = get_stock  # type: ignore[attr-defined]
pl.DataFrame.get_spot = get_spot  # type: ignore[attr-defined]
