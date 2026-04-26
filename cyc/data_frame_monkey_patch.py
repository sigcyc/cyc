from typing import Optional
import shutil
import numpy as np
import polars as pl
import altair as alt
from numba import njit
from .marble import marble
from .gui import PlotSpec

pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED")
pl.Config.set_float_precision(2)
pl.Config.set_tbl_cols(10)
pl.Config.set_tbl_rows(15)
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


def _estimate_col_widths(df: pl.DataFrame, fmt_str_lengths: int) -> list[int]:
    """Estimate display width of each column by sampling rows."""
    PADDING = 2
    float_precision = pl.Config.state(if_set=True).get("set_float_precision")
    sample = pl.concat([df.head(10), df.tail(10)]) if len(df) > 20 else df
    widths = []
    for col in sample.columns:
        s = sample[col]
        header_w = len(col) + PADDING
        if s.dtype.is_float() and float_precision is not None:
            str_col = s.round(float_precision).cast(pl.String).fill_null("null")
        else:
            str_col = s.cast(pl.String).fill_null("null")
        raw_max = str_col.str.len_chars().max()
        max_len = min(raw_max if isinstance(raw_max, int) else 0, fmt_str_lengths)
        data_w = max_len + PADDING
        widths.append(max(header_w, data_w, 5))
    return widths


def _print_all(df: pl.DataFrame) -> None:
    """Print the entire DataFrame, grouping columns by estimated width to fit terminal."""
    terminal_width = get_terminal_size()
    if len(df) > 10000:
        raise ValueError("more than 10k rows")
    df = df.with_columns(pl.col(pl.Datetime).dt.replace_time_zone(None))
    fmt_str_lengths = 100
    col_widths = _estimate_col_widths(df, fmt_str_lengths)

    groups: list[list[str]] = []
    start = 0
    while start < len(df.columns):
        total = 1  # rightmost border
        end = start
        while end < len(df.columns):
            w = col_widths[end] + 1  # column width + border char
            if total + w > terminal_width and end > start:
                break
            total += w
            end += 1
        groups.append(df.columns[start:end])
        start = end

    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=terminal_width,
        fmt_str_lengths=fmt_str_lengths,
    ):
        for i, cols in enumerate(groups):
            print(repr(df.select(cols)))
            if i < len(groups) - 1:
                print()


def _des(df: pl.DataFrame) -> pl.DataFrame:
    return df.describe(percentiles=(0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99))


def _plot(
    df: pl.DataFrame,
    left_axis: list[int | str],
    right_axis: Optional[list[int | str]] = None,
    width: int = 600,
) -> PlotSpec:
    """
    Plot columns on the left/right y-axes against `df.time`.

    Returns a PlotSpec that renders as an altair chart in notebooks and supports
    `+` to combine sources with shared left/right y-scales and color cycling.

    Args:
        left_axis: column index or name to plot on the left y-axis
        right_axis: column index or name to plot on the right y-axis
    """
    right_axis = right_axis or []
    left_cols = [df.columns[i] if isinstance(i, int) else i for i in left_axis]
    right_cols = [df.columns[i] if isinstance(i, int) else i for i in right_axis]
    return PlotSpec([(df, left_cols, right_cols)], width=width)


def _to_df(df: pl.DataFrame, df_type="default"):
    from .df import Df

    return Df(df, df_type).enrich()

def _sort_cut(df: pl.DataFrame, cut_name):
    try:
        return df.unnest(cut_name).sort('breakpoint').drop('breakpoint')
    except Exception:
        raise ValueError(f"{cut_name!r}: pass include_breaks=True to pl.cut")


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
setattr(pl.DataFrame, "des", _des)
setattr(pl.DataFrame, "p", _plot)
setattr(pl.DataFrame, "to_df", _to_df)
setattr(pl.DataFrame, "marble", marble)
setattr(pl.DataFrame, "sort_cut", _sort_cut)
