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
