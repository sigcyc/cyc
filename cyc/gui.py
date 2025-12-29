import polars as pl
import altair as alt
import numpy as np


def gs(x: pl.Series, y: pl.Series, k: int = 20, filter=None):
    """
    Plot a graph with the following
    1. A linear regression line of x, y and add coefficient, intercept, R2 on the graph
    2. Divide x into k buckets. For each bucket, plot the point average(x) and average(y)

    x, y can be very big (>1M points). so the efficiency is vital
    """
    df = pl.DataFrame({"x": x, "y": y}).drop_nulls()
    if filter is not None:
        df = df.filter(filter)
    x_arr = df["x"].to_numpy()
    y_arr = df["y"].to_numpy()

    # Linear regression via normal equations
    x_mean, y_mean = x_arr.mean(), y_arr.mean()
    x_diff = x_arr - x_mean
    ss_xx = (x_diff**2).sum()
    ss_xy = (x_diff * (y_arr - y_mean)).sum()
    ss_yy = ((y_arr - y_mean) ** 2).sum()

    coef = ss_xy / ss_xx if ss_xx else 0
    intercept = y_mean - coef * x_mean
    r2 = (ss_xy**2) / (ss_xx * ss_yy) if ss_xx and ss_yy else 0

    # Bucket aggregation in polars
    x_min, x_max = x_arr.min(), x_arr.max()
    bucket_width = (x_max - x_min) / k if x_max > x_min else 1

    bucketed = (
        df.with_columns(
            ((pl.col("x") - x_min) / bucket_width)
            .floor()
            .cast(pl.Int64)
            .clip(0, k - 1)
            .alias("bucket")
        )
        .group_by("bucket")
        .agg(pl.col("x").mean().alias("x"), pl.col("y").mean().alias("y"))
        .sort("bucket")
    )

    line_df = pl.DataFrame({
        "x": [x_min, x_max],
        "y": [coef * x_min + intercept, coef * x_max + intercept],
    })

    points = alt.Chart(bucketed).mark_circle(size=60).encode(
        x=alt.X("x:Q", title=x.name, scale=alt.Scale(zero=False)),
        y=alt.Y("y:Q", title=y.name, scale=alt.Scale(zero=False)),
        tooltip=["x:Q", "y:Q"]
    )
    line = alt.Chart(line_df).mark_line(color="red", strokeWidth=2).encode(
        x="x:Q", y="y:Q"
    )

    title = f"y = {coef:.4g}x + {intercept:.4g}, R² = {r2:.4f}"
    return (points + line).properties(width=600, height=400, title=title).interactive()
