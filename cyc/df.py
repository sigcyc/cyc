from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Any, Optional, TypedDict, cast
import numpy as np
import polars as pl
import shutil
import altair as alt
import yaml

from .time_util import parse_time_to_ns

pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED")
alt.renderers.enable("browser")


def get_terminal_size():
    return shutil.get_terminal_size().columns - 5


def _print_transpose(df: pl.DataFrame) -> None:
    """Return the full column representation without altering global config."""
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=get_terminal_size(),
    ):
        print(repr(df.head(3).transpose(include_header=True)))


def _print_all(
    df: pl.DataFrame,
    float_precision: Optional[int] = 4,
    fmt_str_lengths: Optional[int] = 100,
) -> None:
    """Print the entire DataFrame content in terminal-width-sized chunks."""
    terminal_width = get_terminal_size()
    chunk_size = max(1, terminal_width // 12)
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=terminal_width,
        float_precision=float_precision,
        fmt_str_lengths=fmt_str_lengths,
    ):
        for start in range(0, len(df.columns), chunk_size):
            cols = df.columns[start : start + chunk_size]
            print(repr(df.select(cols)))
            if start + chunk_size < len(df.columns):
                print()


def _plot(
    self,
    left_axis: list[int],
    right_axis: Optional[list[int]] = None,
    width=600,
    time_format=alt.Undefined,
) -> alt.LayerChart:
    """
    Use alt chart that
    1. use self.time as x-axis
    2. Plot the columns in left_axis on the left y-axis
    3. Plot the columns in right-axis on the right y-axis

    Args:
        left_axis: list of column index to plot on the left y-axis
        right_axis: list of column index to plot on the right y-axis
    """
    right_axis = right_axis or []
    left_cols = [
        self.columns[i + 2] for i in left_axis
    ]  # +2 because the first two columns are sym, time
    right_cols = [self.columns[i + 2] for i in right_axis]

    base = (
        alt.Chart(self)
        .encode(x=alt.X(f"time:T", axis=alt.Axis(format=time_format)))
        .properties(width=width)
    )

    tooltip = [
        alt.Tooltip(f"time:T", title="time"),
        alt.Tooltip("series:N", title="series"),
        alt.Tooltip("value:Q", title="value"),
    ]

    left_chart = (
        base.transform_fold(
            cast(list[str | alt.FieldName], left_cols), as_=["series", "value"]
        )
        .mark_line()
        .encode(
            y=alt.Y(
                "value:Q",
                axis=alt.Axis(title=",".join(left_cols), orient="left"),
                scale=alt.Scale(zero=False),
            ),
            color="series:N",
            tooltip=tooltip,
        )
    )
    right_chart = (
        base.transform_fold(
            cast(list[str | alt.FieldName], right_cols), as_=["series", "value"]
        )
        .mark_line()
        .encode(
            y=alt.Y(
                "value:Q",
                axis=alt.Axis(title=",".join(right_cols), orient="right"),
                scale=alt.Scale(zero=False),
            ),
            color="series:N",
            tooltip=tooltip,
        )
    )

    return (left_chart + right_chart).resolve_scale(y="independent", color="shared")



setattr(pl.DataFrame, "_T", property(_print_transpose))
setattr(pl.DataFrame, "_A", property(_print_all))
setattr(pl.DataFrame, "p", _plot)

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


class Df:
    """
    Attribute:
        time: pl.Datetime("ns")
    """

    df: pl.DataFrame
    df_type: str

    def __init__(self, df: pl.DataFrame, df_type = "default") -> None:
        self.df = df
        self.df_type = df_type

    def enrich(self) -> "Df":
        df_type_dict = get_df_type_dict(self.df_type)
        expr = []
        if not "sym" in self.df.columns:
            expr.append(pl.col(df_type_dict["sym"]).alias("sym"))
        if not "time" in self.df.columns:
            expr.append(
                pl.col(df_type_dict["time"]).cast(pl.Datetime("ns")).alias("time")
            )
        self.df = self.df.with_columns(expr)
        return self

    def s(
        self,
        sym: Optional[str] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        o: Optional[list[str]] = None,  # options in df_types.yaml
        c: Optional[list[str] | str] = None,  # column names
        r: Optional[str] = None,  # regular expression
        f: Optional[np.ndarray] = None,
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
            filters.append(
                time_since_midnight
                >= pl.duration(nanoseconds=parse_time_to_ns(time_start))
            )
        if time_end is not None:
            filters.append(
                time_since_midnight
                <= pl.duration(nanoseconds=parse_time_to_ns(time_end))
            )

        if date is not None:
            if "-" in date:
                start_str, end_str = date.split("-", 1)
                start_date = datetime.strptime(start_str.strip(), "%Y%m%d").date()
                end_date = datetime.strptime(end_str.strip(), "%Y%m%d").date()
                filters.append(
                    (pl.col("time").dt.date() >= start_date)
                    & (pl.col("time").dt.date() <= end_date)
                )
            else:
                date_value = datetime.strptime(date, "%Y%m%d").date()
                filters.append(pl.col("time").dt.date() == date_value)

        if filters:
            combined = filters[0]
            for condition in filters[1:]:
                combined = combined & condition
            df = df.filter(combined)
        if f is not None:
            df = df.filter(f)
        return Df(df, self.df_type)


    def __getattr__(self, name: str):
        attr = getattr(self.df, name)        
        # if attr is a function that returns pl.DataFrame
        # return a wrapper around the function that returns Df on the DataFrame
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, pl.DataFrame):
                    self.df = result
                    return self
                return result
            return wrapper
        return attr

    def __getitem__(self, item):
        result = self.df[item]
        if isinstance(result, pl.DataFrame):
            return Df(result, self.df_type)
        return result

    def __dir__(self):
        return dir(self.df)

    def __repr__(self):
        return repr(self.df)

