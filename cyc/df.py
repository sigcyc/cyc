from __future__ import annotations

import functools
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Optional, ParamSpec, overload

import polars as pl

from .config import get_calendar, get_df_type_dict
from .data_analysis import accum_ratio, accum_ratiop
from .data_finance import add_spot, add_stock
from .data_loaders import load_data
from .types import SymType
from .util_time import calendar_time_zone, parse_time_to_ns


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
    # Shared names get a "_2" twin right after the df1 column; the rest of df2
    # follows at the end, df1 order preserved.
    shared = set(df1.columns) & set(df2.columns)
    rest = [c for c in df2.columns if c not in shared]
    df2 = df2.rename({c: f"{c}_2" for c in shared})
    order = []
    for c in df1.columns:
        order += [c, f"{c}_2"] if c in shared else [c]
    order += rest
    return pl.concat([df1, df2], how="horizontal").select(order)

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

    def __init__(self, df: pl.DataFrame, df_type: str = "default") -> None:
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
                raise ValueError("Sidecar length doesn't match spine")
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
        time_zone = calendar_time_zone(get_calendar(df_type))
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
                    pl.col(time_col).cast(pl.Datetime("ns")).dt.convert_time_zone(time_zone).alias("time")
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
                pl.col("date").cast(pl.Datetime("ns")).dt.replace_time_zone(time_zone).alias("time")
            )
        return lf

    add_stock = wrap_df_func(add_stock)
    add_spot = wrap_df_func(add_spot)
    accum_ratio = wrap_df_func(accum_ratio)
    accum_ratiop = wrap_df_func(accum_ratiop)
    concat_df2 = wrap_df_func(concat_df2)

    def s(
        self,
        sym: SymType = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        o: Optional[list[str]] = None,  # column groups in df_types.yaml
        r: Optional[str] = None,  # regular expression
        a: Optional[list[str | pl.Series | pl.Expr]] = None,  # extra columns/exprs, always shown
        f: pl.Series | pl.Expr = pl.lit(True),
        date: Optional[str] = None,
        n: Optional[int] = None,
    ) -> Df:
        """
        Select columns down to sym + time + whatever o/a/r name, then filter rows.

        Rows are kept where every condition holds:
        1. sym matches if sym is not None
        2. time-of-day >= time_start if given
        3. time-of-day <= time_end if given
        4. date within date if given ("20250102" or "20250102-20250105")
        5. f, an arbitrary boolean expression

        Args:
            sym: "TSLA" or a list of symbols
            time_start: "9:40" or "9:40:03.5"
            time_end: "9:40" or "9:40:03.5"
            date: "20250102"
            o: column groups from df_types.yaml
            r: regex; matching columns are added to the selection
            a: with_columns expressions, always shown (names existing columns too)
            n: sample n rows and re-sort by time
        """
        df_type_dict = get_df_type_dict(self.df_type)
        groups = [col for group in o or [] for col in df_type_dict["cols"][group]]
        # No selector at all -> show every column; otherwise sym/time plus the
        # columns o and a name. r adds its regex matches on top either way.
        pinned = [name for name in ("sym", "time") if name in self.df.columns]
        selected = [*groups] if (o or r or a) else self.df.columns
        col_list = list(dict.fromkeys([*pinned, *selected]))

        select_exprs: list[str | pl.Series | pl.Expr] = [pl.col(name) for name in col_list]
        output_names = set(col_list)
        exclude_names = set(col_list)

        def next_output_name(name: str) -> str:
            if name not in output_names:
                output_names.add(name)
                return name
            suffix = 2
            while (candidate := f"{name}_{suffix}") in output_names:
                suffix += 1
            output_names.add(candidate)
            return candidate

        for expr in a or []:
            if isinstance(expr, str):
                name = expr
                a_expr: pl.Series | pl.Expr = pl.col(expr)
            elif isinstance(expr, pl.Series):
                name = expr.name
                a_expr = expr
            else:
                name = expr.meta.output_name()
                a_expr = expr

            output_name = next_output_name(name)
            exclude_names.add(name)
            exclude_names.add(output_name)
            select_exprs.append(a_expr.alias(output_name) if output_name != name else a_expr)

        df = self.df.select(
            pl.selectors.matches(r or "$^").exclude(exclude_names),
            *select_exprs,
        )

        filters = [f]
        if "sym" in df.columns:
            filters.append(filter_sym(sym))

        if "time" in df.columns:
            time_since_midnight = pl.col("time") - pl.col("time").dt.truncate("1d")
            if time_start is not None:
                filters.append(time_since_midnight >= pl.duration(nanoseconds=parse_time_to_ns(time_start)))
            if time_end is not None:
                filters.append(time_since_midnight <= pl.duration(nanoseconds=parse_time_to_ns(time_end)))

        if date is not None:
            date_expr = pl.col("date") if "date" in df.columns else pl.col("time").dt.date()
            if "-" in date:
                start_str, end_str = date.split("-", 1)
                start_date = datetime.strptime(start_str.strip(), "%Y%m%d").date()
                end_date = datetime.strptime(end_str.strip(), "%Y%m%d").date()
                filters.append((date_expr >= start_date) & (date_expr <= end_date))
            else:
                date_value = datetime.strptime(date, "%Y%m%d").date()
                filters.append(date_expr == date_value)

        df = df.filter(*filters)
        if n is not None:
            df = df.sample(n=n).sort("time")
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

    @overload
    def __getitem__(self, item: str) -> pl.Series: ...
    @overload
    def __getitem__(self, item: Any) -> Any: ...
    def __getitem__(self, item):  # type: ignore[override]
        result = self.df[item]
        if isinstance(result, pl.DataFrame):
            return Df(result, self.df_type)
        return result

    def __dir__(self):
        # Enables Tab-completion in IPython/Jupyter for Df and Polars methods
        return set(super().__dir__()) | set(dir(self.df))

    def __repr__(self):
        return repr(self.df)
