from typing import Sequence
from functools import reduce
import tempfile
import webbrowser

import polars as pl
import treescope

from .util_system import is_notebook


def marble(df: pl.DataFrame, rows: Sequence[int] = (), columns: Sequence[int] = (), sliders: Sequence[int] = ()):
    """
    Render a DataFrame as a treescope array visualization.

    Args:
        df: A long-format Polars DataFrame whose axis columns (those named by
            columns/rows/sliders) uniquely identify each row, plus a value column.
        columns: Indices of columns whose unique values become the column axis.
        rows: Indices of columns whose unique values become the row axis.
        sliders: Indices of columns whose unique values become slider dimensions.

    Value column: not chosen by name — it is read positionally as the last column
    after the axis keys are cross-joined into a grid and df is joined back on (see
    below). That is the last non-axis column of df; if df has more than one non-axis
    column, only this last one is plotted and the rest are silently ignored.

    The axis columns must uniquely identify each row (one row per axis-value
    combination). Otherwise the left join produces multiple rows per cell and the
    final reshape raises a size-mismatch error.
    """

    all_axes = list(columns) + list(rows) + list(sliders)

    # Array axis `position` holds df column all_axes[position]; the caller's df
    # column indices are remapped to those positions for treescope below.
    axis_names = [df.columns[i] for i in all_axes]
    key_df_list = [df.select(name).unique().sort(name) for name in axis_names]
    shapes = [len(key_df) for key_df in key_df_list]
    axis_labels = dict(enumerate(axis_names))
    axis_item_labels = {
        position: key_df.to_series().cast(pl.String).to_list() for position, key_df in enumerate(key_df_list)
    }

    # maintain_order="left": the reshape assumes rows stay in cross-product order.
    df = reduce(lambda a, b: a.join(b, how="cross"), key_df_list).join(
        df, on=axis_names, how="left", maintain_order="left"
    )
    data = df[:, -1].reshape(tuple(shapes)).to_numpy()

    fig = treescope.render_array(
        data,
        columns=list(range(len(columns))),
        rows=list(range(len(columns), len(columns) + len(rows))),
        sliders=list(range(len(columns) + len(rows), len(all_axes))),
        axis_labels=axis_labels,
        axis_item_labels=axis_item_labels,
    )
    if is_notebook():
        return fig
    # Terminal: write the figure's own HTML (the expanded, self-contained array viz
    # that a notebook shows inline) and open it. render_to_html instead wraps the
    # figure in treescope's page chrome, which is what hides it behind a collapsible.
    html_file = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8")
    html_file.write(f"<!doctype html>\n<meta charset='utf-8'>\n{fig._repr_html_()}")
    html_file.close()
    webbrowser.open(f"file://{html_file.name}")
    return html_file.name
