from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

from cyc.ds import Ds


def test__T_returns_full_column_representation():
    n_rows = 5
    n_cols = 500

    data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
    df = Ds(data)

    result = df._T


def test_ds_s():
    df = Ds.load_data('20241211', 'polygon_test')
    filtered = df.s(
        sym="UBER",
        time_start="09:05",
        time_end="09:07",
        col_names=["price"],
        date="20241211",
    )

    pl.DataFrame(df)

    assert isinstance(filtered, Ds)
    assert filtered.columns == ["sym", "time", "price"]
    assert filtered.shape == (3, 3)

def test_load_data():
    df = Ds.load_data('20241211-20241213', 'polygon_test')


def test_ds_p():
    df = Ds.load_data('20241211-20241213', 'polygon_test')
    chart = df.p(left_axis=[0], right_axis=[1])
    assert chart is not None
