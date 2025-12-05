from __future__ import annotations
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from typing import Optional, TypedDict, cast
import numpy as np
import polars as pl
import shutil
import altair as alt
import yaml

from .time_util import parse_time_to_ns, parse_dates

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


setattr(pl.DataFrame, "T", property(_print_transpose))
setattr(pl.DataFrame, "A", property(_print_all))


class DsType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]


def _get_ds_type_dict(ds_type: str) -> DsType:
    """
    Load yaml file from cyc/files/ds_types.yaml. Return the entry with ds_type
    """
    ds_types_path = Path(__file__).resolve().parent / "files" / "ds_types.yaml"
    with ds_types_path.open("r", encoding="utf-8") as file:
        ds_types = yaml.safe_load(file) or {}
    return ds_types[ds_type]


class Ds(pl.DataFrame):
    """
    Attribute:
        time: pl.Datetime("ns")
    """

    _ds_type: str

    def i(self, ds_type: str) -> "Ds":
        ds_type_dict = _get_ds_type_dict(ds_type)
        ds = Ds(
            self.with_columns(
                [
                    pl.col(ds_type_dict["sym"]).alias("sym"),
                    pl.col(ds_type_dict["time"]).cast(pl.Datetime("ns")).alias("time"),
                ]
            )
        )
        ds._ds_type = ds_type
        return ds

    @classmethod
    def load_data_single(cls, ds_type: str):
        data_path = _get_ds_type_dict(ds_type)["data"]["path"]
        return cls(pl.read_parquet(Path(data_path) / f"{ds_type}.parquet")).i(ds_type)

    @classmethod
    def load_data(cls, date_str: str, ds_type: str):
        data_path = _get_ds_type_dict(ds_type)["data"]["path"]
        date_list = parse_dates(date_str)
        data_root = (Path(data_path) / ds_type).expanduser()
        if not data_root.exists():
            raise FileNotFoundError(f"Data path '{data_root}' does not exist")

        if not date_list:
            raise ValueError(f"No trading days found in range '{date_str}'")

        frames: list[pl.DataFrame] = []
        missing_dates: list[str] = []

        for date in tqdm(date_list):
            file_path = data_root / f"{date}.parquet"
            if not file_path.exists():
                missing_dates.append(date)
                continue
            frames.append(pl.read_parquet(file_path))

        if missing_dates:
            print("missing_dates:" + ", ".join(missing_dates))

        combined = pl.concat(frames, how="vertical_relaxed", rechunk=True)
        return cls(combined).i(ds_type)

    def s(
        self,
        sym: Optional[str] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        col_groups: Optional[list[str]] = None,
        col_names: Optional[list[str]] = None,
        f: Optional[np.ndarray] = None,
        date: Optional[str] = None,
    ) -> "Ds":
        """
        Filter the columns to sym + time + col_names, then

        Filter Ds by
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
        col_list = ["sym", "time"]
        col_list_cumsum = []

        ds_type_dict = _get_ds_type_dict(self._ds_type)
        names = []
        for col_group in col_groups or []:
            names += ds_type_dict["cols"][col_group]
        names += col_names or []
        names = names or self.columns

        for col_name in names:
            name, *op = col_name.split(":")
            if not name in col_list:
                col_list.append(name)
                if len(op) == 0:
                    continue
                col_list_cumsum.append(name)

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
            date_value = datetime.strptime(date, "%Y%m%d").date()
            filters.append(pl.col("time").dt.date() == date_value)

        df = self.select(col_list)
        if filters:
            combined = filters[0]
            for condition in filters[1:]:
                combined = combined & condition
            df = df.filter(combined)
        if f is not None:
            df = df.filter(f)
        df = df.with_columns([pl.col(name).cum_sum() for name in col_list_cumsum])
        return self.__class__(df).i(self._ds_type)

    def p(
        self,
        left_axis: list[int],
        right_axis: Optional[list[int]] = None,
        width=600,
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

        base = alt.Chart(self).encode(x=f"time:T").properties(width=width)

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

    def __getattr__(self, name: str):
        if name in self.columns:
            return self[name]
        return getattr(super(), name)
