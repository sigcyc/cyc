from __future__ import annotations

from datetime import datetime
from typing import Literal, cast

import polars as pl
import altair as alt
from sklearn.linear_model import LinearRegression


Source = tuple[pl.DataFrame, list[str], list[str]]  # (df, left_cols, right_cols)


class PlotSpec:
    """Deferred altair chart that composes via `+`.

    `df1.p([0], [1]) + df2.p([0], [1])` stacks each source's columns into a
    single LayerChart: every left column shares one y-scale, every right column
    shares another, and colors cycle across all series.
    """

    def __init__(self, sources: list[Source], width: int = 600):
        self.sources = sources
        self.width = width

    def __add__(self, other):
        if not isinstance(other, PlotSpec):
            return NotImplemented
        return PlotSpec(self.sources + other.sources, self.width)

    def _build(self) -> alt.TopLevelMixin:
        labels = _derive_labels(self.sources)
        left, right, series = [], [], []
        for label, (df, lcs, rcs) in zip(labels, self.sources):
            # VegaFusion treats naive datetimes as local_tz; stripping the zone makes
            # local-in/local-out cancel so face values render unchanged.
            df = df.with_columns(pl.col("time").dt.replace_time_zone(None))
            # Downsample per source — vega/browser chokes well before a million points.
            if len(df) > 10_000:
                df = df.sample(10_000).sort("time")
            for col in lcs:
                name = f"{label}:{col}" if label else col
                series.append(name)
                left.append(_stack(df, col, name))
            for col in rcs:
                name = f"{label}:{col}" if label else col
                series.append(name)
                right.append(_stack(df, col, name))

        color = alt.Scale(domain=series, scheme="category10")
        time_format = self._resolve_time_format(left + right)
        # One altair layer per side, each fed pre-filtered data. 
        sides: list[tuple[list[pl.DataFrame], Literal["left", "right"]]] = [(left, "left"), (right, "right")]
        layers = [
            self._layer(pl.concat(parts), orient, time_format, color)
            for parts, orient in sides
            if parts
        ]
        if len(layers) == 1:
            return layers[0]
        # y=independent gives left and right their own scales (dual-axis);
        # color=shared keeps one legend and one color cycle across both layers.
        return (layers[0] + layers[1]).resolve_scale(y="independent", color="shared")

    def _resolve_time_format(self, parts: list[pl.DataFrame]) -> str:
        t = pl.concat([p["time"] for p in parts])
        tmin = cast(datetime, t.min())
        tmax = cast(datetime, t.max())
        return "%Y%m%d" if tmin.date() != tmax.date() else "%H:%M:%S"

    def _layer(self, data: pl.DataFrame, orient: Literal["left", "right"], time_format: str, color: alt.Scale) -> alt.Chart:
        titles = data["series"].unique(maintain_order=True).to_list()
        return (
            alt.Chart(data)
            .mark_line()
            .encode(
                x=alt.X("time:T", axis=alt.Axis(format=time_format, labelAngle=-45)),
                y=alt.Y(
                    "value:Q",
                    axis=alt.Axis(title=",".join(titles), orient=orient),
                    # zero=False: price series never touch 0, so forcing it wastes axis range.
                    scale=alt.Scale(zero=False),
                ),
                color=alt.Color("series:N", scale=color),
                tooltip=[
                    alt.Tooltip("time:T", title="time"),
                    alt.Tooltip("series:N", title="series"),
                    alt.Tooltip("value:Q", title="value"),
                ],
            )
            .properties(width=self.width)
        )

    def _repr_mimebundle_(self, include=None, exclude=None):
        return self._build()._repr_mimebundle_(include, exclude)

    def __getattr__(self, name: str):
        # Forward public attrs (.properties, .save, .interactive, ...) to the built chart.
        # Block dunders/privates: __getattr__ fires during pickle, copy, and altair's
        # internal pydantic lookups — delegating those to a rebuilt chart would either
        # recurse or return the wrong object's internals.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._build(), name)

    def __repr__(self) -> str:
        return f"PlotSpec(sources={len(self.sources)}, width={self.width})"


def _stack(df: pl.DataFrame, col: str, name: str) -> pl.DataFrame:
    """Select (time, series=name, value=col) from `df` — one series in long format."""
    return df.select(
        pl.col("time"),
        pl.lit(name).alias("series"),
        pl.col(col).cast(pl.Float64).alias("value"),
    )


def _derive_labels(sources: list[Source]) -> list[str]:
    """Per-source disambiguation prefix; empty string means no prefix.

    Strategy: if a source has a single unique sym we prefer that (readable legend
    like "TSLA:price"); otherwise we fall back to the source index. If sym-based
    labels aren't unique across sources (e.g. two TSLA frames) we go all-index,
    since half-labeled legends would be more confusing than uniformly indexed.
    """
    # Single source: preserve the original single-df behavior — bare column names.
    if len(sources) == 1:
        return [""]
    labels = []
    for idx, (df, _, _) in enumerate(sources):
        sym_label = None
        if "sym" in df.columns:
            syms = df["sym"].unique().to_list()
            if len(syms) == 1:
                sym_label = str(syms[0])
        labels.append(sym_label or f"#{idx}")
    if len(set(labels)) != len(labels):
        return [f"#{i}" for i in range(len(sources))]
    return labels


def gs(x: pl.Series, y: pl.Series, k: int = 10, filter=None) -> alt.LayerChart:
    """
    Plot a graph with the following
    1. A linear regression line of x, y and add coefficient, intercept, R2 on the graph
    2. Divide x into k buckets. For each bucket, plot the point average(x) and average(y)

    x, y can be very big (>1M points). so the efficiency is vital
    """
    df = pl.DataFrame({"x": x, "y": y})
    if filter is not None:
        df = df.filter(filter)
    df = df.drop_nulls()
    x_arr = df["x"].to_numpy().reshape(-1, 1)
    y_arr = df["y"].to_numpy()

    model = LinearRegression().fit(x_arr, y_arr)
    coef, intercept = model.coef_[0], model.intercept_
    r2 = model.score(x_arr, y_arr)

    # Bucket aggregation in polars
    bucketed = (
        df.with_columns(
            ((pl.col("x").rank() - 1) * k // pl.len()).alias("bucket")
        )
        .group_by("bucket")
        .agg(pl.col("x").mean().alias("x"), pl.col("y").mean().alias("y"))
        .sort("bucket")
    )
    points = (
        alt.Chart(bucketed)
        .mark_circle(size=60)
        .encode(
            x=alt.X("x:Q", title=x.name, scale=alt.Scale(zero=False)),
            y=alt.Y("y:Q", title=y.name, scale=alt.Scale(zero=False)),
            tooltip=["x:Q", "y:Q"],
        )
    )

    x_min, x_max = df["x"].min(), df["x"].max()
    line_df = pl.DataFrame(
        {
            "x": [x_min, x_max],
            "y": [coef * x_min + intercept, coef * x_max + intercept],
        }
    )
    line = (
        alt.Chart(line_df)
        .mark_line(color="red", strokeWidth=2)
        .encode(x="x:Q", y="y:Q")
    )

    title = f"y = {coef:.4g}x + {intercept:.4g}, R² = {r2:.4f}, n = {len(df):,}"
    return (points + line).properties(width=600, height=400, title=title).interactive()
