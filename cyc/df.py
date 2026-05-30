from __future__ import annotations
import functools
from datetime import datetime
from itertools import zip_longest
from typing import Any, Optional, TYPE_CHECKING, Callable, Concatenate, ParamSpec
from .types import SymType
import polars as pl
from .data_finance import add_stock, add_spot
from .data_analysis import accum_ratiop, accum_ratio
from .config import get_df_type_dict

from .data_loaders import load_data
from .time_util import parse_time_to_ns


_DfBase = pl.DataFrame if TYPE_CHECKING else object

P = ParamSpec("P")


def filter_sym(sym: SymType) -> pl.Expr:
    if sym is None:
        return pl.lit(True)
    syms = [sym] if isinstance(sym, (str, int)) else list(sym)
    return pl.col("sym").is_in(syms)

def concat_df2(df1: pl.DataFrame | Df, df2: pl.DataFrame | Df) -> pl.DataFrame:
    if isinstance(df1, Df):
        df1 = df1.df
    if isinstance(df2, Df):
        df2 = df2.df
    df1 = df1.rename({c: f"{c}_x" for c in df1.columns})
    df2 = df2.rename({c: f"{c}_y" for c in df2.columns})
    interleaved = [c for pair in zip_longest(df1.columns, df2.columns) for c in pair if c is not None]
    return pl.concat([df1, df2], how="horizontal").select(interleaved)

def wrap_df_func(
    func: Callable[Concatenate[pl.DataFrame, P], Any],
) -> Callable[Concatenate[Df, P], Any]:
    @functools.wraps(func)
    def wrapper(df: Df, *args: P.args, **kwargs: P.kwargs) -> Any:
        result = func(df.df, *args, **kwargs)
        if isinstance(result, pl.DataFrame):
            return Df(result, df.df_type)
        return result

    return wrapper


class Df(_DfBase):
    """
    Attribute:
        time: pl.Datetime("ns")
    """

    df: pl.DataFrame  # unmodifiable except in enrich / load_sidecars
    df_type: str

    def __init__(self, df: pl.DataFrame, df_type="default") -> None:
        self.df = df
        self.df_type = df_type

    @classmethod
    def load_data(cls, df_type: str, date_str: str | pl.Series | None = None, sym: SymType = None) -> Df:
        lf = load_data(df_type, date_str, sym)
        lf = cls._enrich(lf, df_type)
        lf = lf.filter(filter_sym(sym))  # after _enrich so the canonical "sym" column exists
        return Df(lf.collect(), df_type)

    def load_sidecars(self, sidecars: str | list[str]) -> None:
        """
        Load each sidecar and merge its columns into self.df.

        sym/time in the sidecar are verified against self.df rather than
        overwritten. Where the sidecar value is not null, it must equal
        self.df's value or ValueError is raised.

        Not supported if self.df was sym-filtered at load: sidecar parquets
        carry all syms for the date range, so rows won't align (ShapeError).
        """
        if isinstance(sidecars, str):
            sidecars = [sidecars]
        date_list = self.df["date"].unique() if "date" in self.df.columns else None
        protected = ("sym", "time", "date")

        for name in sidecars:
            sidecar_df = load_data(f"{self.df_type}__{name}", date_list, None).collect()
            if len(sidecar_df) != len(self.df):
                self_counts = self.df.group_by("date").len().sort("date")
                sidecar_counts = sidecar_df.group_by("date").len().sort("date")
                diff = (
                    self_counts.join(
                        sidecar_counts, on="date", how="full", coalesce=True, suffix="_sidecar"
                    )
                    .filter(pl.col("len").ne_missing(pl.col("len_sidecar")))
                    .sort("date")
                )
                print(diff)
                raise ValueError(f"Sidecar length doesn't match spine")
            new_columns = []
            for col in sidecar_df.get_columns():
                if col.name in protected and col.name in self.df.columns:
                    if (col.is_not_null() & col.ne_missing(self.df[col.name])).any():
                        raise ValueError(
                            f"Sidecar {name!r} column {col.name!r} does not match self.df"
                        )
                else:
                    new_columns.append(col)
            self.df = self.df.with_columns(new_columns)

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
        col_list = [c for c in ["sym", "time"] if c in df.columns]
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
        if "sym" in df.columns:
            filters.append(filter_sym(sym))

        if "time" in df.columns:
            time_since_midnight = pl.col("time") - pl.col("time").dt.truncate("1d")
            if time_start is not None:
                filters.append(time_since_midnight >= pl.duration(nanoseconds=parse_time_to_ns(time_start)))
            if time_end is not None:
                filters.append(time_since_midnight <= pl.duration(nanoseconds=parse_time_to_ns(time_end)))

        if date is not None:
            if "date" in df.columns:
                date_expr = pl.col('date')
            else:
                date_expr = pl.col("time").dt.date()
            if "-" in date:
                start_str, end_str = date.split("-", 1)
                start_date = datetime.strptime(start_str.strip(), "%Y%m%d").date()
                end_date = datetime.strptime(end_str.strip(), "%Y%m%d").date()
                filters.append((date_expr >= start_date) & (date_expr <= end_date))
            else:
                date_value = datetime.strptime(date, "%Y%m%d").date()
                filters.append(date_expr == date_value)

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
                args = tuple(a.df if isinstance(a, Df) else a for a in args)
                kwargs = {k: v.df if isinstance(v, Df) else v for k, v in kwargs.items()}
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
