import polars as pl
import altair as alt
from sklearn.linear_model import LinearRegression


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
