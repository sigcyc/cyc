from typing import Optional, cast
import shutil
import numpy as np
import polars as pl
import altair as alt
from numba import njit
from .marble import marble

FLOAT_PRECISION = 2
pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED")
pl.Config.set_float_precision(FLOAT_PRECISION)
alt.data_transformers.enable("vegafusion")
try:
    get_ipython()  # type: ignore
    alt.renderers.enable("browser")
    def get_terminal_size():
        return shutil.get_terminal_size().columns - 5
except NameError:
    def get_terminal_size():
        return 200


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
    if len(df) > 10000:
        raise ValueError("more than 10k rows")
    df = df.with_columns(pl.col(pl.Datetime).dt.replace_time_zone(None))
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
        else:
            time_format = "%H:%M:%S"

    # VegaFusion interprets naive datetimes in local_tz by default.
    # Using the default local scale means local-in/local-out cancel, preserving face values.
    df = self.with_columns(pl.col("time").dt.replace_time_zone(None))
    if len(df) > 10_000:
        df = df.sample(10_000).sort("time")
    base = (
        alt.Chart(df)
        .encode(x=alt.X(f"time:T", axis=alt.Axis(format=time_format, labelAngle=-45)))
        .properties(width=width)
    )

    tooltip = [
        alt.Tooltip(f"time:T", title="time"),
        alt.Tooltip("series:N", title="series"),
        alt.Tooltip("value:Q", title="value"),
    ]

    left_chart = (
        base.transform_fold(cast(list[str | alt.FieldName], left_cols), as_=["series", "value"])
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
        base.transform_fold(cast(list[str | alt.FieldName], right_cols), as_=["series", "value"])
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


def _to_df(df: pl.DataFrame, df_type="default"):
    from .df import Df

    return Df(df, df_type).enrich()


@njit(cache=True)
def _ewm_sum(value: np.ndarray, time: np.ndarray, alpha: float) -> np.ndarray:
    """
    Args:
        alpha: the constant used in the exponential. It should match the scale in time
            e.g. -ln(2) / half_life_us
    """
    n = len(value)
    result_arr = np.zeros(n)
    if n == 0:
        return result_arr
    result_arr[0] = value[0]
    for i in range(1, n):
        result_arr[i] = result_arr[i - 1] * np.exp((time[i] - time[i - 1]) * alpha) + value[i]
    return result_arr


@pl.api.register_expr_namespace("cyc")
class Cyc:

    def __init__(self, value) -> None:
        self._value = value

    def ewm_sum(self, time: pl.Expr, half_life_us: float):
        """
        Args:
            half_life_us: in us as to match dt.timestamp() default
        """
        alpha = -np.log(2) / half_life_us
        return pl.struct([self._value, time]).map_batches(
            lambda s: _ewm_sum(s.struct[0].to_numpy(), s.struct[1].dt.timestamp().to_numpy(), alpha)
        )


setattr(pl.DataFrame, "_T", property(_print_transpose))
setattr(pl.DataFrame, "_A", property(_print_all))
setattr(pl.DataFrame, "p", _plot)
setattr(pl.DataFrame, "to_df", _to_df)
setattr(pl.DataFrame, "marble", marble)
