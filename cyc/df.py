from __future__ import annotations
import functools
from pathlib import Path
from datetime import datetime
from typing import Optional, TypedDict, TYPE_CHECKING, Callable, Concatenate, ParamSpec
import polars as pl
import yaml
from .data_finance import add_stock, add_spot
from .data_analysis import accum_ratiop, accum_ratio

from .data_loaders import load_data, load_data_single
from .time_util import parse_time_to_ns


class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]


def get_df_type_dict(df_type: str) -> DfType:
    """
    Load yaml file from cyc/files/df_types.yaml. Return the entry with df_type
    """
    df_types_path = Path(__file__).resolve().parent / "files" / "df_types.yaml"
    with df_types_path.open("r", encoding="utf-8") as file:
        df_types = yaml.safe_load(file) or {}
    return df_types[df_type]


_DfBase = pl.DataFrame if TYPE_CHECKING else object

P = ParamSpec("P")


def wrap_df_func(
    func: Callable[Concatenate[pl.DataFrame, P], pl.DataFrame],
) -> Callable[Concatenate[Df, P], Df]:
    @functools.wraps(func)
    def wrapper(df: Df, *args: P.args, **kwargs: P.kwargs) -> Df:
        result = func(df.df, *args, **kwargs)
        return Df(result, df.df_type)

    return wrapper


class Df(_DfBase):
    """
    Attribute:
        time: pl.Datetime("ns")
    """

    df: pl.DataFrame  # unmodifiable except in enrich
    df_type: str

    def __init__(self, df: pl.DataFrame, df_type="default") -> None:
        self.df = df
        self.df_type = df_type

    @classmethod
    def load_data_single(cls, df_type: str) -> Df:
        return Df(load_data_single(df_type), df_type).enrich()

    @classmethod
    def load_data(cls, date_str: str | pl.Series, df_type: str) -> Df:
        return Df(load_data(date_str, df_type), df_type).enrich()

    def enrich(self) -> "Df":
        df_type_dict = get_df_type_dict(self.df_type)
        expr = []
        if not "sym" in self.df.columns:
            expr.append(pl.col(df_type_dict["sym"]).alias("sym"))
        if not "time" in self.df.columns:
            expr.append(
                pl.col(df_type_dict["time"])
                .cast(pl.Datetime("ns"))
                .dt.convert_time_zone("America/New_York")
                .alias("time")
            )
        self.df = self.df.with_columns(expr)
        return self

    add_stock = wrap_df_func(add_stock)
    add_spot = wrap_df_func(add_spot)
    accum_ratio = wrap_df_func(accum_ratio)
    accum_ratiop = wrap_df_func(accum_ratiop)

    def s(
        self,
        sym: Optional[str] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        o: Optional[list[str]] = None,  # options in df_types.yaml
        c: Optional[list[str] | str] = None,  # column names
        r: Optional[str] = None,  # regular expression
        f: pl.Series | pl.Expr = pl.lit(True),
        date: Optional[str] = None,
    ) -> "Df":
        """
        Filter the columns to sym + time + col_names, then

        Filter Df by
        1. self.sym == sym is sym is not None
        2. self.time is greater than time_start if time_start is not None
        3. self.time is less than time_end if time_end is not None
        3. date of self.time equal to date if date is not None

        col_names: list of column names. We support operation on column names when the name contains ":".
        For example, if the name is "volume:cumsum", then the function will run a cumsum on that column

        Args:
            sym: TSLA
            time_start: "9:40" or "9:40:03.5"
            time_end: "9:40" or "9:40:03.5"
            date: "20250102"
        """
        df = self.df
        col_list = ["sym", "time"]
        col_list_cumsum = []

        df_type_dict = get_df_type_dict(self.df_type)
        names = []
        for col_group in o or []:
            names += df_type_dict["cols"][col_group]
        c = [c] if isinstance(c, str) else c
        names += c or []
        if not (o or c or r):
            names = df.columns

        for col_name in names:
            name, *op = col_name.split(":")
            if not name in col_list:
                col_list.append(name)
                if len(op) == 0:
                    continue
                col_list_cumsum.append(name)

        df = df.select(
            pl.selectors.by_name(col_list),
            pl.selectors.matches(r or "$^").exclude(col_list),
        )
        df = df.with_columns([pl.col(name).cum_sum() for name in col_list_cumsum])

        filters = []
        if sym is not None:
            filters.append(pl.col("sym") == sym)

        time_since_midnight = pl.col("time") - pl.col("time").dt.truncate("1d")
        if time_start is not None:
            filters.append(time_since_midnight >= pl.duration(nanoseconds=parse_time_to_ns(time_start)))
        if time_end is not None:
            filters.append(time_since_midnight <= pl.duration(nanoseconds=parse_time_to_ns(time_end)))

        if date is not None:
            if "-" in date:
                start_str, end_str = date.split("-", 1)
                start_date = datetime.strptime(start_str.strip(), "%Y%m%d").date()
                end_date = datetime.strptime(end_str.strip(), "%Y%m%d").date()
                filters.append((pl.col("time").dt.date() >= start_date) & (pl.col("time").dt.date() <= end_date))
            else:
                date_value = datetime.strptime(date, "%Y%m%d").date()
                filters.append(pl.col("time").dt.date() == date_value)

        df = df.filter(f, *filters)
        return Df(df, self.df_type)

    def to_pl(self) -> pl.DataFrame:
        return self.df

    def __getattr__(self, name: str):
        attr = getattr(self.df, name)
        # if attr is a function that returns pl.DataFrame
        # return a wrapper around the function that returns Df on the DataFrame
        if callable(attr):

            @functools.wraps(attr)
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, pl.DataFrame):
                    return Df(result, self.df_type)
                return result

            return wrapper
        return attr

    def __getitem__(self, item):  # type: ignore[override]
        result = self.df[item]
        if isinstance(result, pl.DataFrame):
            return Df(result, self.df_type)
        return result

    def __dir__(self):
        # Enables Tab-completion in IPython/Jupyter for Polars methods
        return set(dir(super()) + dir(self._df))

    def __repr__(self):
        return repr(self.df)
