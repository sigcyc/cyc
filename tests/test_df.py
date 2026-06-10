from datetime import datetime, timedelta

import altair as alt
import numpy as np
import polars as pl
import pytest

from cyc.df import Df
from cyc.data_loaders import load_data
from cyc.gui import PlotSpec


def test__T_returns_full_column_representation():
    n_rows = 5
    n_cols = 500

    data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
    df = Df(pl.DataFrame(data))

    result = df._T


def test_df_s():
    df = Df.load_data("polygon_test", "20241211")
    filtered = df.s(
        sym="UBER",
        time_start="09:05",
        time_end="09:07",
        c=["price"],
        date="20241211",
    )

    pl.DataFrame(df)

    assert isinstance(filtered, Df)
    assert filtered.columns == ["sym", "time", "price"]
    assert filtered.shape == (3, 3)


def test_load_data():
    df = load_data("polygon_test", "20241211-20241213")


def test_df_p():
    df = load_data("polygon_test", "20241211-20241213").collect()
    chart = df.p(left_axis=[0], right_axis=[1])
    assert chart is not None


class TestPlotSpec:
    @pytest.fixture(autouse=True)
    def _plain_transformer(self):
        prev = alt.data_transformers.active
        alt.data_transformers.enable("default")
        yield
        alt.data_transformers.enable(prev)

    def _make_df(self, sym: str = "TSLA", n: int = 5) -> pl.DataFrame:
        t = [datetime(2024, 1, 1) + timedelta(seconds=i) for i in range(n)]
        return pl.DataFrame({
            "sym": [sym] * n,
            "time": t,
            "a": [float(i) for i in range(n)],
            "b": [10.0 + i for i in range(n)],
        })

    def test_single_source_returns_plotspec(self):
        spec = self._make_df().p(["a"], ["b"])
        assert isinstance(spec, PlotSpec)
        assert len(spec.sources) == 1

    def test_single_source_legend_has_bare_column_names(self):
        spec = self._make_df().p(["a"], ["b"])
        d = spec._build().to_dict()
        domain = d["layer"][0]["encoding"]["color"]["scale"]["domain"]
        assert domain == ["a", "b"]

    def test_single_source_left_only_skips_right_layer(self):
        d = self._make_df().p(["a"])._build().to_dict()
        # Single-source, left-only: one line layer + the crosshair rule.
        # No right-axis line layer, no dual-axis resolve.
        assert len(d["layer"]) == 2
        assert "resolve" not in d
        orients = [lyr.get("encoding", {}).get("y", {}).get("axis", {}).get("orient") for lyr in d["layer"]]
        assert "right" not in orients

    def test_add_merges_sources(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("UBER").p(["a"], ["b"])
        combined = a + b
        assert isinstance(combined, PlotSpec)
        assert len(combined.sources) == 2

    def test_combined_uses_sym_as_label(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("UBER").p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        domain = d["layer"][0]["encoding"]["color"]["scale"]["domain"]
        assert domain == ["TSLA_a", "TSLA_b", "UBER_a", "UBER_b"]

    def test_combined_falls_back_to_index_when_syms_collide(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("TSLA").p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        domain = d["layer"][0]["encoding"]["color"]["scale"]["domain"]
        assert domain == ["#0_a", "#0_b", "#1_a", "#1_b"]

    def test_combined_resolves_left_and_right_independently(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("UBER").p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        assert d["resolve"]["scale"]["y"] == "independent"
        assert d["resolve"]["scale"]["color"] == "shared"

    def test_combined_left_layer_contains_all_left_series(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("UBER").p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        left, right, _crosshair = d["layer"]
        assert left["encoding"]["y"]["axis"]["orient"] == "left"
        assert right["encoding"]["y"]["axis"]["orient"] == "right"
        # both TSLA_a and UBER_a live on the same layer → same y-scale
        assert left["encoding"]["y"]["axis"]["title"] == "TSLA_a,UBER_a"
        assert right["encoding"]["y"]["axis"]["title"] == "TSLA_b,UBER_b"

    def test_combined_layer_data_split_by_side(self):
        a = self._make_df("TSLA", n=3).p(["a"], ["b"])
        b = self._make_df("UBER", n=3).p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        datasets = d["datasets"]
        left_name = d["layer"][0]["data"]["name"]
        right_name = d["layer"][1]["data"]["name"]
        left_series = {r["series"] for r in datasets[left_name]}
        right_series = {r["series"] for r in datasets[right_name]}
        assert left_series == {"TSLA_a", "UBER_a"}
        assert right_series == {"TSLA_b", "UBER_b"}
        # 2 sources × 1 col per side × 3 rows
        assert len(datasets[left_name]) == 6
        assert len(datasets[right_name]) == 6

    def test_crosshair_rule_has_tooltip_for_every_series(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("UBER").p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        crosshair = d["layer"][-1]
        assert crosshair["mark"]["type"] == "rule"
        fields = [t["field"] for t in crosshair["encoding"]["tooltip"]]
        # time first, then one row per series
        assert fields == ["time", "TSLA_a", "TSLA_b", "UBER_a", "UBER_b"]

    def test_filter_tuple_plots_column_per_filter(self):
        df = pl.concat([self._make_df("TSLA"), self._make_df("UBER")])
        spec = df.p([
            ("a", (pl.col("sym") == "TSLA").alias("TSLA")),
            ("a", (pl.col("sym") == "UBER").alias("UBER")),
        ])
        d = spec._build().to_dict()
        domain = d["layer"][0]["encoding"]["color"]["scale"]["domain"]
        assert domain == ["a_TSLA", "a_UBER"]
        data = d["datasets"][d["layer"][0]["data"]["name"]]
        tsla = [r["value"] for r in data if r["series"] == "a_TSLA"]
        # masked nulls are dropped at stack time: interleaved nulls would break
        # the vega line path into invisible single-point fragments
        assert tsla == [0.0, 1.0, 2.0, 3.0, 4.0]

    def test_filter_tuple_default_name_and_int_index(self):
        df = pl.concat([self._make_df("TSLA"), self._make_df("UBER")])
        # column 2 is "a"; unaliased filter names the series after its root column
        spec = df.p([(2, pl.col("sym") == "TSLA")])
        src, left_cols, _ = spec.sources[0]
        assert left_cols == ["a_sym"]
        # rows where every plotted column is null are dropped up front
        assert len(src) == 5
        assert src["a_sym"].null_count() == 0

    def test_filter_tuple_mixes_with_plain_columns(self):
        df = pl.concat([self._make_df("TSLA"), self._make_df("UBER")])
        spec = df.p([("a", (pl.col("sym") == "TSLA").alias("TSLA"))], ["b"])
        src, left_cols, right_cols = spec.sources[0]
        assert (left_cols, right_cols) == (["a_TSLA"], ["b"])
        # "b" is non-null everywhere, so no row is all-null and none are dropped
        assert len(src) == 10

    def test_filter_tuples_with_colliding_names_raise(self):
        df = pl.concat([self._make_df("TSLA"), self._make_df("UBER")])
        with pytest.raises(pl.exceptions.PolarsError, match="duplicate"):
            df.p([("a", pl.col("sym") == "TSLA"), ("a", pl.col("sym") == "UBER")])

    def test_delegates_altair_methods_via_getattr(self):
        spec = self._make_df().p(["a"], ["b"])
        # .properties is an altair method; __getattr__ routes through _build()
        assert callable(spec.properties)

    def test_add_with_non_plotspec_returns_notimplemented(self):
        spec = self._make_df().p(["a"], ["b"])
        assert spec.__add__(42) is NotImplemented


class TestDfGetattr:
    def test_column_access_priority_over_parent_attr(self):
        """Column access takes priority when column name matches a parent attribute."""
        # 'columns' is an attribute of pl.DataFrame, but if we have a column named 'columns'
        # the __getattr__ should not be triggered since 'columns' exists on parent
        obj = Df(pl.DataFrame({"a": [1, 2, 3]}))

        # 'columns' should return the DataFrame columns list, not go through __getattr__
        assert obj.columns == ["a"]

    def test_nonexistent_attr_raises_error(self):
        """Accessing a nonexistent attribute should raise AttributeError."""
        obj = Df(pl.DataFrame({"a": [1, 2, 3]}))

        try:
            _ = obj.nonexistent_column
            assert False, "Should have raised AttributeError"
        except AttributeError:
            pass
