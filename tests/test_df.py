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
        chart = self._make_df().p(["a"])._build()
        assert not isinstance(chart, alt.LayerChart)

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
        assert domain == ["TSLA:a", "TSLA:b", "UBER:a", "UBER:b"]

    def test_combined_falls_back_to_index_when_syms_collide(self):
        a = self._make_df("TSLA").p(["a"], ["b"])
        b = self._make_df("TSLA").p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        domain = d["layer"][0]["encoding"]["color"]["scale"]["domain"]
        assert domain == ["#0:a", "#0:b", "#1:a", "#1:b"]

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
        left, right = d["layer"]
        assert left["encoding"]["y"]["axis"]["orient"] == "left"
        assert right["encoding"]["y"]["axis"]["orient"] == "right"
        # both TSLA:a and UBER:a live on the same layer → same y-scale
        assert left["encoding"]["y"]["axis"]["title"] == "TSLA:a,UBER:a"
        assert right["encoding"]["y"]["axis"]["title"] == "TSLA:b,UBER:b"

    def test_combined_layer_data_split_by_side(self):
        a = self._make_df("TSLA", n=3).p(["a"], ["b"])
        b = self._make_df("UBER", n=3).p(["a"], ["b"])
        d = (a + b)._build().to_dict()
        datasets = d["datasets"]
        left_name = d["layer"][0]["data"]["name"]
        right_name = d["layer"][1]["data"]["name"]
        left_series = {r["series"] for r in datasets[left_name]}
        right_series = {r["series"] for r in datasets[right_name]}
        assert left_series == {"TSLA:a", "UBER:a"}
        assert right_series == {"TSLA:b", "UBER:b"}
        # 2 sources × 1 col per side × 3 rows
        assert len(datasets[left_name]) == 6
        assert len(datasets[right_name]) == 6

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
