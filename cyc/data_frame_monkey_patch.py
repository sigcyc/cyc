from typing import Optional, cast
import shutil
import polars as pl
import altair as alt
from marble import marble

FLOAT_PRECISION = 2
pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED")
pl.Config.set_float_precision(FLOAT_PRECISION)
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
    float_precision: Optional[int] = FLOAT_PRECISION,
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
    left_axis: list[int | str],
    right_axis: Optional[list[int | str]] = None,
    width=600,
    time_format=alt.Undefined,
) -> alt.LayerChart:
    """
    Use alt chart that
    1. use self.time as x-axis
    2. Plot the columns in left_axis on the left y-axis
    3. Plot the columns in right-axis on the right y-axis

    Args:
        left_axis: list of column index or name to plot on the left y-axis
        right_axis: list of column index or name to plot on the right y-axis
    """
    right_axis = right_axis or []
    left_cols = [self.columns[i] if isinstance(i, int) else i for i in left_axis]
    right_cols = [self.columns[i] if isinstance(i, int) else i for i in right_axis]

    if time_format is alt.Undefined:
        min_time = self["time"].min()
        max_time = self["time"].max()
        if (min_time.year, min_time.month) != (max_time.year, max_time.month):
            time_format = "%Y%m%d"

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


def _to_df(df: pl.DataFrame, df_type='default'):
    from .df import Df
    return Df(df, df_type).enrich()



setattr(pl.DataFrame, "_T", property(_print_transpose))
setattr(pl.DataFrame, "_A", property(_print_all))
setattr(pl.DataFrame, "p", _plot)
setattr(pl.DataFrame, "to_df", _to_df)
setattr(pl.DataFrame, "marble", marble)
