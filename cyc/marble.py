from typing import Sequence
from functools import reduce
import tempfile
import webbrowser

import polars as pl
import treescope

from .util_system import is_notebook


def marble(df: pl.DataFrame, columns: Sequence[int] = (), rows: Sequence[int] = (), sliders: Sequence[int] = ()):
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

    # Collect all grouping columns in order: sliders, rows, columns
    all_axes = list(columns) + list(rows) + list(sliders)

    key_df_list = [pl.DataFrame()] * len(all_axes)
    shapes = [-1] * len(all_axes)
    axis_item_labels = {}
    axis_labels = {}

    for i in all_axes:
        col_name = df.columns[i]
        key_df = df.select(col_name).unique().sort(col_name)
        key_df_list[i] = key_df
        shapes[i] = len(key_df)
        axis_item_labels[i] = key_df.to_series().cast(pl.String).to_list()
        axis_labels[i] = col_name

    # create keys
    df = reduce(lambda a, b: a.join(b, how="cross"), key_df_list).join(df, on=list(axis_labels.values()), how="left")
    data = df[:, -1].reshape(tuple(shapes)).to_numpy()

    fig = treescope.render_array(
        data,
        columns=columns,
        rows=rows,
        sliders=sliders,
        axis_labels=axis_labels,
        axis_item_labels=axis_item_labels,
    )
    if is_notebook():
        return fig
    # Terminal: write the figure's own HTML (the expanded, self-contained array viz
    # that a notebook shows inline) and open it. render_to_html instead wraps the
    # figure in treescope's page chrome, which is what hides it behind a collapsible.
    path = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8")
    path.write(f"<!doctype html>\n<meta charset='utf-8'>\n{fig._repr_html_()}")
    path.close()
    webbrowser.open(f"file://{path.name}")
    return path.name


if __name__ == "__main__":
    # saved_path = save_render_array_example()
    # print(f"Saved Treescope render to {saved_path.resolve()}")

    df = pl.DataFrame(
        {
            "cat": ["A", "A", "B", "B", "A"],
            "grp": ["X", "Y", "X", "Y", "X"],
            "num": [10, 20, 30, 40, 10],
        }
    )
    df.group_by(["cat", "grp"]).agg(pl.col("num").first())
