from datetime import datetime
from typing import Optional
import polars as pl
import shutil
import altair as alt

from .util import parse_time_to_ns

pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED")
alt.renderers.enable("browser")


def get_terminal_size():
    return shutil.get_terminal_size().columns - 5


class Ds(pl.DataFrame):
    """
    Attribute:
        time: pl.Datetime("ns")
    """

    @property
    def _T(self):
        """Return the full column representation without altering global config."""
        with pl.Config(
            tbl_rows=-1,
            tbl_cols=-1,
            tbl_width_chars=get_terminal_size(),
        ):
            print(repr(self.head(3).transpose(include_header=True)))

    @property
    def _A(self, float_precision: Optional[int] = 4):
        terminal_width = get_terminal_size()
        chunk_size = max(1, terminal_width // 12)
        with pl.Config(
            tbl_rows=-1,
            tbl_cols=-1,
            tbl_width_chars=terminal_width,
            float_precision=float_precision,
        ):
            for start in range(0, len(self.columns), chunk_size):
                cols = self.columns[start : start + chunk_size]
                print(repr(self.select(cols)))
                if start + chunk_size < len(self.columns):
                    print()

    def s(
        self,
        sym: Optional[str],
        time_start: Optional[str],
        time_end: Optional[str],
        col_names: list[str],
        date: Optional[str],
    ):
        """
        Filter the columns to sym + time + col_names, then

        Filter Ds by
        1. self.sym == sym is sym is not None
        2. self.time is greater than time_start if time_start is not None
        3. self.time is less than time_end if time_end is not None
        3. date of self.time equal to date if date is not None

        Args:
            sym: TSLA
            time_start: "9:40" or "9:40:03.5"
            time_end: "9:40" or "9:40:03.5"
            date: "20250102"
        """
        selected_cols = []
        for name in ["sym", "time", *col_names]:
            if name not in selected_cols:
                selected_cols.append(name)

        filters = []
        if sym is not None:
            filters.append(pl.col("sym") == sym)

        time_since_midnight = pl.col("time") - pl.col("time").dt.truncate("1d")
        if time_start is not None:
            filters.append(time_since_midnight >= pl.duration(nanoseconds=parse_time_to_ns(time_start)))
        if time_end is not None:
            filters.append(time_since_midnight <= pl.duration(nanoseconds=parse_time_to_ns(time_end)))

        if date is not None:
            date_value = datetime.strptime(date, "%Y%m%d").date()
            filters.append(pl.col("time").dt.date() == date_value)

        df: pl.DataFrame = self
        if filters:
            combined = filters[0]
            for condition in filters[1:]:
                combined = combined & condition
            df = df.filter(combined)

        df = df.select(selected_cols)
        return self.__class__(df)


    def __getattr__(self, name):
        if name in self.columns:
            return self[name]
        return getattr(super(), name)
