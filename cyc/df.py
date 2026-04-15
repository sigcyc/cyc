from __future__ import annotations
import functools
from datetime import datetime
from typing import Optional, TYPE_CHECKING, Callable, Concatenate, ParamSpec
from .types import SymType
import polars as pl
from .data_finance import add_stock, add_spot
from .data_analysis import accum_ratiop, accum_ratio
from .config import get_df_type_dict

from .data_loaders import load_data, load_data_single, load_data_hive_sym
from .config import get_file_layout
from .time_util import parse_time_to_ns


_DfBase = pl.DataFrame if TYPE_CHECKING else object

P = ParamSpec("P")


def filter_sym(lf: pl.LazyFrame, sym: SymType) -> pl.LazyFrame:
    if sym is None:
        return lf
    if isinstance(sym, (str, int)):
        return lf.filter(pl.col("sym") == sym)
    return lf.filter(pl.col("sym").is_in(list(sym)))


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
    def load_data(cls, df_type: str, date_str: str | pl.Series | None = None, sym: SymType = None) -> Df:
        file_layout = get_file_layout(df_type)
        match file_layout:
            case "hive_sym":
                assert date_str is not None
                lf = load_data_hive_sym(df_type, date_str, sym)
            case "single" | "single_hive_sym":
                lf = load_data_single(df_type)
            case _:
                assert date_str is not None
                lf = load_data(df_type, date_str)

        lf = cls._enrich(lf, df_type)
        lf = filter_sym(lf, sym)
        return Df(lf.collect(), df_type)

    @classmethod
    def _enrich(cls, lf: pl.LazyFrame, df_type: str) -> pl.LazyFrame:
        df_type_dict = get_df_type_dict(df_type)
        schema = lf.collect_schema()
        columns = schema.names()
        expr = []

        if "sym" not in columns:
            expr.append(pl.col(df_type_dict["sym"]).alias("sym"))

        has_time = "time" in columns
        has_date = "date" in columns

        if not has_time and (time_col := df_type_dict.get("time")):
            if isinstance(schema[time_col], pl.Datetime):
                expr.append(pl.col(time_col).alias("time"))
            else:
                expr.append(
                    pl.col(time_col).cast(pl.Datetime("ns")).dt.convert_time_zone("America/New_York").alias("time")
                )
            has_time = True

        if not has_date and (date_col := df_type_dict.get("date")):
            expr.append(pl.col(date_col).alias("date"))
            has_date = True

        lf = lf.with_columns(expr)

        if has_time and has_date:
            return lf
        if has_time:
            return lf.with_columns(pl.col("time").dt.date().alias("date"))
        if has_date:
            return lf.with_columns(
                pl.col("date").cast(pl.Datetime("ns")).dt.replace_time_zone("America/New_York").alias("time")
            )
        return lf

    add_stock = wrap_df_func(add_stock)
    add_spot = wrap_df_func(add_spot)
    accum_ratio = wrap_df_func(accum_ratio)
    accum_ratiop = wrap_df_func(accum_ratiop)

    def s(
        self,
        sym: SymType = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        o: Optional[list[str]] = None,  # options in df_types.yaml
        c: Optional[list[str] | str] = None,  # column names
        r: Optional[str] = None,  # regular expression
        f: pl.Series | pl.Expr = pl.lit(True),
        date: Optional[str] = None,
        sample: Optional[int] = None,
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
            if isinstance(sym, (str, int)):
                filters.append(pl.col("sym") == sym)
            else:
                filters.append(pl.col("sym").is_in(sym))

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
        if sample is not None:
            df = df.sample(n=sample).sort("time")
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
