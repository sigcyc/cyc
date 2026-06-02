from __future__ import annotations

import polars as pl

from .time_util import next_trading_day, previous_trading_day
from .data_loaders import load_data


def add_stock(self: pl.DataFrame, sym: str, date: str, fields: str | list[str] | dict[str, str]) -> pl.DataFrame:
    """
    Join stock_data_day fields onto self by (sym, date).

    Args:
        sym: name of the symbol column in self
        date: name of the date column in self
        fields: field(s) to fetch — a column name, a list of names, or a
            {source_field: name} mapping to rename on the way in

    Returns:
        DataFrame with sym, date, and requested fields
    """
    if isinstance(fields, str):
        mapping = {fields: fields}
    elif isinstance(fields, dict):
        mapping = fields
    else:
        mapping = {f: f for f in fields}
    stock_data = load_data("stock_data_day", self[date].unique()).collect()
    stock_data = stock_data.select(
        pl.col("ticker").alias(sym),
        pl.col("date").alias(date),
        *[pl.col(name_old).alias(name_new) for name_old, name_new in mapping.items()],
    )
    return self.join(stock_data, on=[sym, date], how="left")


def add_spot(
    self: pl.DataFrame, sym: str, date: str, num_days: int, field: str = "close", name: str | None = None
) -> pl.DataFrame:
    """
    Get spot price adjusted for dividends and splits.

    Args:
        sym: name of the symbol column in self
        date: name of the date column in self
        num_days: 0 for current, positive for forward, negative for backward
        field: price field to adjust (default: close)
        name: output column name (default: spot_d{num_days}, or spot_dm{n} when negative)

    Returns:
        DataFrame with sym, date, and adjusted field
    """
    result = _get_spot(self[sym], self[date], num_days, field)
    name = name or (f"spot_d{num_days}" if num_days >= 0 else f"spot_dm{-num_days}")
    return self.with_columns(result.alias(name))


def _get_spot(sym: pl.Series, date: pl.Series, num_days: int, field: str) -> pl.Series:
    """Recursively compute adjusted spot price."""
    df = pl.DataFrame({"sym": sym, "date": date})

    if num_days == 0:
        return add_stock(df, "sym", "date", field)[field]

    if num_days > 0:
        next_day = next_trading_day(date)
        spot = _get_spot(sym, next_day, num_days - 1, field)
        next_df = pl.DataFrame({"sym": sym, "date": next_day})
        adj = add_stock(next_df, "sym", "date", ["dividend", "split"])
        dividend = adj["dividend"].fill_null(0)
        split = adj["split"].fill_null(1)
        return spot * split + dividend

    prev_day = previous_trading_day(date)
    spot = _get_spot(sym, prev_day, num_days + 1, field)
    adj = add_stock(df, "sym", "date", ["dividend", "split"])
    dividend = adj["dividend"].fill_null(0)
    split = adj["split"].fill_null(1)
    return (spot - dividend) / split
