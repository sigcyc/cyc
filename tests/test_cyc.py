import polars as pl
import pytest
from datetime import datetime

import cyc.data_frame_monkey_patch  # noqa: F401 - registers the namespace


class TestEwmSum:
    def test_basic_ewm_sum(self):
        """Basic exponential weighted sum with known values."""
        df = pl.DataFrame({
            "time": [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 0, 0, 1),
                datetime(2024, 1, 1, 0, 0, 2),
            ],
            "value": [1.0, 1.0, 1.0],
        })
        half_life_us = 1_000_000  # 1 second

        result = df.select(pl.col("value").cyc.ewm_sum(pl.col("time"), half_life_us))

        values = result["value"].to_list()
        assert values[0] == pytest.approx(1.0)
        assert values[1] == pytest.approx(1.5, rel=1e-6)
        assert values[2] == pytest.approx(1.75, rel=1e-6)

    def test_ewm_sum_single_element(self):
        df = pl.DataFrame({
            "time": [datetime(2024, 1, 1)],
            "value": [42.0],
        })

        result = df.select(pl.col("value").cyc.ewm_sum(pl.col("time"), 1_000_000))

        assert result["value"].to_list() == [42.0]

    def test_ewm_sum_empty_input(self):
        """Empty input should return empty result, not crash."""
        df = pl.DataFrame({
            "time": pl.Series([], dtype=pl.Datetime),
            "value": pl.Series([], dtype=pl.Float64),
        })

        result = df.select(pl.col("value").cyc.ewm_sum(pl.col("time"), 1_000_000))

        assert result.shape == (0, 1)

    def test_ewm_sum_large_time_gap(self):
        """Large time gap decays previous sum to near zero."""
        df = pl.DataFrame({
            "time": [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 0, 1, 0),  # 60 seconds later
            ],
            "value": [1000.0, 1.0],
        })
        half_life_us = 1_000_000  # 1 second

        result = df.select(pl.col("value").cyc.ewm_sum(pl.col("time"), half_life_us))

        values = result["value"].to_list()
        assert values[0] == 1000.0
        assert values[1] == pytest.approx(1.0, abs=1e-10)


class TestCut:
    def _df(self):
        return pl.DataFrame({
            "close":  [5.0, 150.0, 50.0, 500.0, 250.0, -10.0],
            "volume": [1,   2,     3,    4,     5,     6],
        })

    def _cats(self, df):
        """Bin labels in original row order."""
        return df["close_cut"].struct.field("category").to_list()

    def test_default_keeps_all(self):
        """Without f, every row is binned and no filtered bucket appears."""
        out = self._df().select(pl.col("close").cyc.cut([0, 100, 300]))
        assert out.height == 6
        assert out.columns == ["close_cut"]
        assert "filtered" not in self._cats(out)

    def test_filter_preserves_length(self):
        """f does not drop rows; excluded rows land in the filtered bucket."""
        out = self._df().select(pl.col("close").cyc.cut([0, 100, 300], f=pl.col("close") > 0))
        assert out.height == 6  # -10.0 kept, not dropped
        assert self._cats(out)[5] == "filtered"  # the -10.0 row

    def test_usable_in_with_columns(self):
        """Length is preserved, so cut can sit alongside full-length columns."""
        out = self._df().with_columns(pl.col("close").cyc.cut([0, 100, 300], f=pl.col("volume") >= 4))
        assert out.height == 6
        # rows with volume < 4 (the first three) are filtered out
        assert self._cats(out) == ["filtered", "filtered", "filtered", "(300, inf]", "(100, 300]", "(-inf, 0]"]

    def test_sort_cut_unfiltered(self):
        """sort_cut orders bins by breakpoint, low bin first."""
        out = self._df().select(pl.col("close").cyc.cut([0, 100, 300]))
        cats = out.sort_cut("close_cut")["close_cut"].to_list()
        assert cats == ["(-inf, 0]", "(0, 100]", "(0, 100]", "(100, 300]", "(100, 300]", "(300, inf]"]

    def test_sort_cut_puts_filtered_last(self):
        """The filtered bucket sorts after every real bin."""
        out = self._df().select(pl.col("close").cyc.cut([0, 100, 300], f=pl.col("close") > 0))
        cats = out.sort_cut("close_cut")["close_cut"].to_list()
        assert cats == ["(0, 100]", "(0, 100]", "(100, 300]", "(100, 300]", "(300, inf]", "filtered"]
