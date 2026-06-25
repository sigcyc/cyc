from typing import Optional
import shutil
import numpy as np
import polars as pl
import polars.selectors as cs
import altair as alt
from numba import njit
from .marble import marble
from .gui import PlotSpec, gs
from .util_system import is_notebook

pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED")
pl.Config.set_tbl_cols(10)
pl.Config.set_tbl_rows(15)
alt.data_transformers.enable("vegafusion")
if is_notebook():
    # Notebook renders altair inline; no real terminal, so use a wide default.
    def get_terminal_size():
        return 200

else:
    # Terminal / script: open charts in a browser and use the real terminal width.
    alt.renderers.enable("browser")

    def get_terminal_size():
        return shutil.get_terminal_size().columns - 10


def _print_transpose(df: pl.DataFrame) -> None:
    """Print the full column representation without altering global config."""
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=get_terminal_size(),
    ):
        print(repr(df.head(3).with_columns((cs.binary() | cs.categorical()).cast(pl.String)).transpose(include_header=True)))


def _estimate_col_widths(df: pl.DataFrame, fmt_str_lengths: int) -> list[int]:
    """Display widths, read off Polars' own rendering of what `_print_all` shows.

    Measuring the real render is the only thing that gets every dtype right: a
    plain `cast(String)` does not reproduce how `mixed` mode shows floats (e.g.
    374693089.85 displays as `3.7469e8`, not `374693089.85`), so it can mis-size
    float columns by 2x. See docs/polars_float_display.md.

    The frame measured here is exactly the one `_print_all` renders -- the whole
    data plus the leading `#` index column -- so the budget cannot drift from the
    render. Two ways it used to drift: measuring a sample missed a datetime
    column's finest sub-second precision when it lived only in an unsampled row
    (Polars sizes the column to that row), and the `#` column was never counted.
    The full frame at the 10k-row cap renders in well under 100ms. Returns one
    width per rendered column, `#` first.
    """
    shown = df.select(pl.int_range(pl.len()).alias("#"), pl.all())
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=65535,  # u16 max: never wrap, so each column gets its natural width
        fmt_str_lengths=fmt_str_lengths,
    ):
        lines = repr(shown).splitlines()
    # ASCII_FULL_CONDENSED top border is `+----+----+`; the dash run between each
    # pair of `+` is that column's width (content plus one space of padding each
    # side), which is exactly what `_print_all` budgets per column.
    border = next(line for line in lines if "+" in line and set(line) <= {"+", "-"})
    return [len(segment) for segment in border.strip("+").split("+")]


def _print_all(df: pl.DataFrame) -> None:
    """Print the entire DataFrame, grouping columns by estimated width to fit terminal."""
    terminal_width = get_terminal_size()
    if len(df) > 10000:
        raise ValueError("more than 10k rows")
    df = df.with_columns(pl.col(pl.Datetime).dt.replace_time_zone(None))
    fmt_str_lengths = 100
    # `#` is rendered in every group, so its width is pinned out of the per-group
    # budget up front; col_widths then lines up positionally with df.columns.
    index_width, *col_widths = _estimate_col_widths(df, fmt_str_lengths)

    groups: list[list[str]] = []
    start = 0
    while start < len(df.columns):
        total = 1 + index_width + 1  # rightmost border + the `#` column
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
            print(repr(df.select(pl.int_range(pl.len()).alias("#"), *cols)))
            if i < len(groups) - 1:
                print()


def _des(df: pl.DataFrame) -> pl.DataFrame:
    return df.describe(percentiles=(0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99))


Value = int | str | pl.Expr  # column index, column name, or expression
AxisItem = Value | tuple[Value, pl.Expr]  # a value, or (value, filter) to mask it


def _plot(
    df: pl.DataFrame,
    left_axis: list[AxisItem],
    right_axis: Optional[list[AxisItem]] = None,
    width: int = 600,
) -> PlotSpec:
    """
    Plot columns on the left/right y-axes against `df.time`.

    Returns a PlotSpec that renders as an altair chart in notebooks and supports
    `+` to combine sources with shared left/right y-scales and color cycling.

    Args:
        left_axis: each entry plots one series on the left y-axis. An entry is:
            - a column index or name -> plotted as-is, named by the column
            - a polars expression -> plotted by its output name (alias to rename):
              pl.col("spot").cum_sum().alias("cum") -> "cum"
            - a (value, filter) tuple, where value is a column or an expression ->
              plots the value only where the filter holds, named
              "{value}_{filter_name}" (alias the filter to control the name):
              ("spot", (pl.col("sym") == "TSLA").alias("TSLA")) -> "spot_TSLA"
              (pl.col("bid") - pl.col("ask"), cond) -> the expression masked by cond
        right_axis: same as left_axis, on the right y-axis
    """
    right_axis = right_axis or []

    def resolve(item: AxisItem) -> tuple[str, Optional[pl.Expr]]:
        """Resolve an axis entry to (series_name, expr_to_add).

        expr_to_add is None for a plain column (already in df); otherwise it is
        the expression to compute — masked by the filter for a (value, filter)
        tuple, and named "{value}_{filter}".
        """
        # Separate the optional filter from the value it masks.
        if isinstance(item, tuple):
            value, filter_expr = item
        else:
            value, filter_expr = item, None

        # Resolve the value to (name, expr); a plain column becomes pl.col(name).
        is_expr = isinstance(value, pl.Expr)
        if is_expr:
            name, expr = value.meta.output_name(), value
        else:
            name = df.columns[value] if isinstance(value, int) else value
            expr = pl.col(name)

        # No filter: add bare expressions; a plain column is already in df.
        if filter_expr is None:
            return name, expr if is_expr else None

        # Filter: mask the value, naming the series after both.
        name = f"{name}_{filter_expr.meta.output_name()}"
        return name, pl.when(filter_expr).then(expr).alias(name)

    left = [resolve(item) for item in left_axis]
    right = [resolve(item) for item in right_axis]
    left_cols = [name for name, _ in left]
    right_cols = [name for name, _ in right]
    computed_exprs = [expr for _, expr in left + right if expr is not None]
    # Rows where every plotted column is null paint nothing but eat the
    # downsampling budget in PlotSpec._build, so drop them up front.
    plotted = df.with_columns(computed_exprs).filter(
        pl.any_horizontal(pl.col(c).is_not_null() for c in dict.fromkeys(left_cols + right_cols))
    )
    return PlotSpec([(plotted, left_cols, right_cols)], width=width)


def _sort_cut(df: pl.DataFrame, cut_name):
    if cut_name in df.columns and not isinstance(df.schema[cut_name], pl.Struct):
        raise ValueError(f"{cut_name!r}: pass include_breaks=True to pl.cut")
    return df.unnest(cut_name).sort("breakpoint", maintain_order=True).drop("breakpoint").rename({"category": cut_name})


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

    def cut(self, breaks, f=pl.lit(True), **kwargs):
        """Cut into bins, keeping length. Rows where `f` is false go to the
        `filtered` bucket, which sorts last (NaN breakpoint)."""
        name = self._value.meta.output_name()
        cut = self._value.cut(breaks, include_breaks=True, **kwargs)
        filtered = pl.struct(breakpoint=pl.lit(float("nan")), category=pl.lit("filtered"))
        return pl.when(f).then(cut).otherwise(filtered).alias(f"{name}_cut")


setattr(pl.DataFrame, "_T", property(_print_transpose))
setattr(pl.DataFrame, "_A", property(_print_all))
setattr(pl.DataFrame, "des", _des)
setattr(pl.DataFrame, "p", _plot)
setattr(pl.DataFrame, "marble", marble)
setattr(pl.DataFrame, "sort_cut", _sort_cut)
setattr(pl.DataFrame, "gs", gs)
