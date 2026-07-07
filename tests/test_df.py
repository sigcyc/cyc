from datetime import date, datetime, timedelta

import altair as alt
import numpy as np
import polars as pl
import pytest

import cyc_ref_data
from cyc_ref_data.masters import MASTER_SCHEMAS, write_master

from cyc.df import Df
from cyc.data_loaders import load_data
from cyc.gui import PlotSpec


def test__T_returns_full_column_representation():
    n_rows = 5
    n_cols = 500

    data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
    df = Df(pl.DataFrame(data))

    df._T


def test_df_s():
    df = Df.load_data("polygon_test", "20241211")
    filtered = df.s(
        sym="UBER",
        time_start="09:05",
        time_end="09:07",
        a=["price"],
        date="20241211",
    )

    pl.DataFrame(df)

    assert isinstance(filtered, Df)
    assert filtered.columns == ["sym", "time", "price"]
    assert filtered.shape == (3, 3)


def test_df_s_suffixes_a_duplicate_of_group_column():
    df = Df(pl.DataFrame({"sym": ["A"], "time": [datetime(2024, 1, 1)]}))

    selected = df.s(o=["core"], a=["sym"])

    assert selected.columns == ["sym", "time", "sym_2"]
    assert selected["sym_2"].to_list() == ["A"]


def test_df_s_suffixes_duplicate_a_columns():
    df = Df(pl.DataFrame({"col1": [1, 2]}))

    selected = df.s(a=["col1", "col1", "col1"])

    assert selected.columns == ["col1", "col1_2", "col1_3"]
    assert selected["col1_2"].to_list() == [1, 2]
    assert selected["col1_3"].to_list() == [1, 2]


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

    def test_dual_axis_pins_explicit_height(self):
        # The crosshair rule has no y encoding, so vega-lite sizes it at the 20px
        # default step height. Combined with the dual-axis interactive() restructuring,
        # that mismatch against the 300px line layers collapsed the plot area — the
        # lines vanished and the x-axis jumped to the top. Pinning a shared explicit
        # height on every layer is what keeps them aligned; assert it stays pinned.
        d = self._make_df().p(["a"], ["b"])._build().to_dict()
        assert "resolve" in d  # this is the dual-axis path that used to collapse
        assert d["height"] == 300

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

    def test_filter_tuple_accepts_expression_value(self):
        # (expression, filter): the expression is computed and then masked where
        # the filter holds — the value slot is not limited to a bare column.
        df = self._make_df("TSLA")  # a=[0,1,2,3,4], b=[10,11,12,13,14]
        spec = df.p([((pl.col("a") * pl.col("b")).alias("ab"), (pl.col("a") >= 2).alias("hi"))])
        src, left_cols, _ = spec.sources[0]
        assert left_cols == ["ab_hi"]
        # a*b only where a>=2; the masked (null) rows are dropped up front
        assert src["ab_hi"].to_list() == [24.0, 39.0, 56.0]

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


class TestEnrichInstrumentId:
    @pytest.fixture(autouse=True)
    def masters(self, tmp_path, monkeypatch):
        """Scratch cyc_ref_data masters: AAPL=7, TSLA=8."""
        monkeypatch.setenv("CYC_REF_DATA_MASTERS", str(tmp_path))
        equity = pl.DataFrame(
            {
                "instrument_id": [7, 8],
                "sym": ["AAPL", "TSLA"],
                "kind": ["CS", "CS"],
                "valid_from": [date(2020, 1, 1)] * 2,
                "valid_to": [None, None],
                "first_seen": [date(2020, 1, 1)] * 2,
            },
            schema=MASTER_SCHEMAS["equity"],
        )
        write_master(equity, "equity", tmp_path)
        cyc_ref_data.reload_masters()
        yield
        cyc_ref_data.reload_masters()

    def test_derives_instrument_id_from_sym(self):
        lf = pl.LazyFrame({"ticker": ["TSLA", "AAPL", "UNKNOWN"], "window_start": [datetime(2024, 1, 2, 9, 30)] * 3})

        out = Df._enrich(lf, "stock_data_day").collect()

        assert out["instrument_id"].to_list() == [8, 7, None]
        assert out["instrument_id"].dtype == pl.UInt32

    def test_derives_sym_from_instrument_id(self):
        lf = pl.LazyFrame({"instrument_id": [8, 7], "time": [datetime(2024, 1, 2, 9, 30)] * 2})

        out = Df._enrich(lf, "instrument_id_test").collect()

        assert out["sym"].to_list() == ["TSLA", "AAPL"]

    def test_missing_declared_sym_column_crashes(self):
        # polygon_test declares sym: sym; a frame without it is broken data
        lf = pl.LazyFrame({"instrument_id": [8, 7], "time": [datetime(2024, 1, 2, 9, 30)] * 2})

        with pytest.raises(pl.exceptions.ColumnNotFoundError):
            Df._enrich(lf, "polygon_test").collect()

    def test_existing_sym_is_not_overridden(self):
        lf = pl.LazyFrame({"sym": ["AAPL"], "instrument_id": [8], "time": [datetime(2024, 1, 2, 9, 30)]})

        out = Df._enrich(lf, "polygon_test").collect()

        assert out["sym"].to_list() == ["AAPL"]


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
