# It contains the common utils used in studies

from datetime import datetime
from typing import overload

import polars as pl

from cyc.time_util import next_trading_day, previous_trading_day

from .data_loaders import load_data


@overload
def get_stock(
    sym: pl.Series | str, date: pl.Series | str, fields: list[str]
) -> pl.DataFrame: ...
@overload
def get_stock(
    sym: pl.Series | str, date: pl.Series | str, fields: str
) -> pl.Series: ...
def get_stock(
    sym: pl.Series | str, date: pl.Series | str, fields: list[str] | str
) -> pl.DataFrame | pl.Series:
    """
    Get the stock_data_day for sym date.

    1. Load stock_data_day for the unique days in date
    2. Merge stock_data_day on sym and date
    3. If fields is list, return a dataframe, else return pl.Series
    4. The output is sym, date aligned

    If sym is str, it broadcasts to the size of date.
    If date is str (YYYYMMDD), it broadcasts to the size of sym.
    """
    if isinstance(sym, str) and isinstance(date, str):
        raise ValueError("sym and date cannot both be str")

    if isinstance(sym, str):
        sym = pl.Series("sym", [sym] * len(date))
    if isinstance(date, str):
        date_val = datetime.strptime(date, "%Y%m%d").date()
        date = pl.Series("date", [date_val] * len(sym))

    unique_dates = date.unique()
    stock_data = load_data(unique_dates, "stock_data_day").df

    field_list = [fields] if isinstance(fields, str) else fields
    select_cols = ["sym", "date"] + field_list
    stock_data = stock_data.select(select_cols)

    input_df = pl.DataFrame({"sym": sym, "date": date})
    result = input_df.join(stock_data, on=["sym", "date"], how="left")

    if isinstance(fields, str):
        return result[fields]
    return result.select(field_list)


def get_spot(
    sym: pl.Series | str, date: pl.Series, num_days: int, field="close"
) -> pl.Series:
    if num_days == 0:
        return get_stock(sym, date, field)
    elif num_days > 0:
        next_day = next_trading_day(date)
        dividend, split = get_stock(sym, next_day, ["dividend", "split"])
        spot = get_spot(sym, next_day, num_days - 1, field)
        return spot * split.fill_null(1) + dividend.fill_null(0)
    else:
        prev_day = previous_trading_day(date)
        dividend, split = get_stock(sym, date, ["dividend", "split"])
        spot = get_spot(sym, prev_day, num_days + 1, field)
        return spot / split.fill_null(1) - dividend.fill_null(0)
