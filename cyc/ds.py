from datetime import datetime
from typing import Literal, Optional, cast
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
        for col_name in col_names:
            name, *op = col_name.split(":")
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
        df = df.with_columns([pl.col(name).cum_sum() for name in col_list_cumsum])

        return self.__class__(df)

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

    def __getattr__(self, name):
        if name in self.columns:
            return self[name]
        return getattr(super(), name)
