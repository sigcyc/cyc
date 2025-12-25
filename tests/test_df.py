
import numpy as np
import polars as pl

from cyc.df import Df
from cyc.data_loaders import load_data


def test__T_returns_full_column_representation():
    n_rows = 5
    n_cols = 500

    data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
    df = Df(pl.DataFrame(data))

    result = df._T


def test_df_s():
    df = load_data('20241211', 'polygon_test')
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
    df = load_data('20241211-20241213', 'polygon_test')


def test_df_p():
    df = load_data('20241211-20241213', 'polygon_test')
    chart = df.p(left_axis=[0], right_axis=[1])
    assert chart is not None


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
