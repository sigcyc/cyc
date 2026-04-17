# Change: Redesign Plot UI

## Context

I like altair's declarative syntax that you define the graph as you go. In my workflow, I have a dataframe wrapper `Df`.

Currently, I can use `df.plot([0], [1])` to plot column 0 on the left and 1 on the right, which is what I like.

## Problem

Now, I want to do things like `df1.plot([0], [1]) + df2.plot([0], [1])`, and I want the final graph with left y-axis to share the same scale and the right y-axis to share the same scale. And the color cycle through both of them.

## Approaches

### A. Pure altair (`LayerChart + LayerChart`)

Each call returns an `alt.LayerChart` with `resolve_scale(y="independent")` to split its own left/right axes. Layering two such charts should "just work".

Why it doesn't: Vega-Lite's `resolve_scale` only has `shared` or `independent` per channel — there is no way to group layers so "every layer on the left shares scale S_left and every layer on the right shares scale S_right". We want *partially shared* y-scales. Altair does not expose this.

### B. Escape to raw Vega with named scales

Drop down to Vega JSON, define `scale_left` and `scale_right`, bind marks to named scales. Gives exact control over scale grouping but throws away altair's ergonomics and forces every future tweak through hand-written Vega. Rejected on cost.

### C. Merge data, then build one chart

Concatenate each source into a long-format table `(time, series, value, side)` where `side ∈ {"left", "right"}`. Build *one* `LayerChart`: layer 1 filters `side == "left"`, layer 2 filters `side == "right"`. Each layer has one y-scale, shared across every source that contributed to that side. Color encoding is a single `alt.Color("series:N")` with one scale → colors cycle across all columns from all sources automatically.

Minimal, no new scale plumbing, no Vega escape hatch. The special case (single source) becomes the general case (N sources).

### D. `PlotSpec` deferred object + `+` operator

`df.p(...)` returns a `PlotSpec` holding the source list `[(df, left_cols, right_cols), ...]`. `PlotSpec + PlotSpec` merges the source lists. Rendering (via `_repr_mimebundle_` in notebooks, or explicit `.to_chart()`) runs approach C.

This preserves the user syntax `df1.p(...) + df2.p(...)` exactly, delegates the merge to the point where we have all the information, and keeps altair output for `.properties()`, `.save()`, `.interactive()` etc. via a thin `__getattr__` proxy.

## Recommendation

**D (PlotSpec) on top of C (data merge).** It's the only approach that satisfies all three requirements (shared left-scale, shared right-scale, color cycle across sources) without fighting Vega-Lite's scale resolution model, and it keeps the public API exactly as it is today.

## Implementation sketch

```python
class PlotSpec:
    sources: list[tuple[pl.DataFrame, list[str], list[str]]]
    width: int
    time_format: Any

    def __add__(self, other: "PlotSpec") -> "PlotSpec":
        return PlotSpec(self.sources + other.sources, self.width, self.time_format)

    def _build(self) -> alt.LayerChart:
        # stack all sources into long-format (time, series, value, side)
        # one LayerChart with two transform_filter layers, shared color scale
        ...

    def _repr_mimebundle_(self, include, exclude):
        return self._build()._repr_mimebundle_(include, exclude)

    def __getattr__(self, name):
        return getattr(self._build(), name)  # .properties, .save, .interactive, ...
```

`_plot` becomes a one-liner that constructs a single-source `PlotSpec`.

### Series naming

When multiple sources are combined, column names can collide (`df1` and `df2` both have `"price"`). Disambiguation rule:
- single source → no prefix (current behavior preserved)
- multi-source, each df has a single unique `sym` → prefix with that sym (`"TSLA:price"`)
- otherwise → prefix with source index (`"#0:price"`)
