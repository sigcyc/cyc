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
                name = f"{label}_{col}" if label else col
                series.append(name)
                left.append(_stack(df, col, name))
            for col in rcs:
                name = f"{label}_{col}" if label else col
                series.append(name)
                right.append(_stack(df, col, name))

        color = alt.Scale(domain=series, scheme="tableau20")
        time_format = self._resolve_time_format(left + right)
        # One altair layer per side, each fed pre-filtered data.
        sides: list[tuple[list[pl.DataFrame], Literal["left", "right"]]] = [(left, "left"), (right, "right")]
        layers = [
            self._layer(pl.concat(parts), orient, time_format, color)
            for parts, orient in sides
            if parts
        ]
        crosshair = self._crosshair(pl.concat(left + right), series, time_format)
        if len(layers) == 1:
            chart = layers[0] + crosshair
        else:
            # y=independent gives left and right their own scales (dual-axis);
            # color=shared keeps one legend and one color cycle across both layers.
            # The crosshair rule has no y encoding, so independent y on the line
            # layers doesn't distort it.
            chart = (layers[0] + layers[1] + crosshair).resolve_scale(y="independent", color="shared")
        # bind_y=False: drag-pan and wheel-zoom act on the x scale only.
        return chart.interactive(bind_y=False)

    def _resolve_time_format(self, parts: list[pl.DataFrame]) -> str:
        t = pl.concat([p["time"] for p in parts])
        tmin = cast(datetime, t.min())
        tmax = cast(datetime, t.max())
        return "%Y%m%d" if tmin.date() != tmax.date() else "%H:%M:%S"

    def _layer(self, data: pl.DataFrame, orient: Literal["left", "right"], time_format: str, color: alt.Scale) -> alt.Chart:
        title = ",".join(data["series"].unique(maintain_order=True).to_list())[:100]
        return (
            alt.Chart(data)
            .mark_line()
            .encode(
                x=alt.X("time:T", axis=alt.Axis(format=time_format, labelAngle=-45)),
                y=alt.Y(
                    "value:Q",
                    axis=alt.Axis(title=title, orient=orient),
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

    def _crosshair(self, data: pl.DataFrame, series: list[str], time_format: str) -> alt.Chart:
        """Dashed vertical guide that reveals every series' value at the cursor.

        Pivot in polars (not Vega's `transform_pivot` — VegaFusion fails to
        downcast the pivot column). One row per time, one column per series,
        so the tooltip reads all values straight from the hovered row.
        """
        wide = data.pivot(values="value", index="time", on="series", aggregate_function="last").sort("time")
        hover = alt.selection_point(
            nearest=True, on="pointerover", fields=["time"], empty=False, clear="pointerout"
        )
        return (
            alt.Chart(wide)
            .mark_rule(color="gray", strokeDash=[3, 3])
            .encode(
                x="time:T",
                opacity=alt.when(hover).then(alt.value(0.5)).otherwise(alt.value(0)),
                tooltip=[alt.Tooltip("time:T", title="time", format=time_format)]
                + [alt.Tooltip(f"{s}:Q", title=s) for s in series],
            )
            .add_params(hover)
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
    like "TSLA_price"); otherwise we fall back to the source index. If sym-based
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


def gs(df: pl.DataFrame, x: pl.Expr, y: pl.Expr, k: int = 10, f: pl.Expr = pl.lit(True)) -> alt.LayerChart:
    """
    Plot a graph with the following
    1. A linear regression line of x, y and add coefficient, intercept, R2 on the graph
    2. Divide x into k buckets. For each bucket, plot the point average(x) and average(y)

    df may be very big (>1M rows), so efficiency is vital. x, y, and f are
    expressions evaluated against df; f selects which rows to include.
    """
    x_title, y_title = x.meta.output_name(), y.meta.output_name()
    df = df.filter(f).select(x=x, y=y).drop_nulls()
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
            x=alt.X("x:Q", title=x_title, scale=alt.Scale(zero=False)),
            y=alt.Y("y:Q", title=y_title, scale=alt.Scale(zero=False)),
            tooltip=["x:Q", "y:Q"],
        )
    )

    x_min, x_max = bucketed["x"].min(), bucketed["x"].max()
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
