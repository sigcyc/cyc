import numpy as np
import polars as pl

from cyc.ds import Ds


def test__T_returns_full_column_representation():
    n_rows = 5
    n_cols = 500

    data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
    df = Ds(data)

    result = df._T
