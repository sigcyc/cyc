import polars as pl
from cyc.marble import marble

def test_marble(monkeypatch):
    opened = []
    monkeypatch.setattr("webbrowser.open", opened.append)

    df = pl.DataFrame({
        "cat": ["A", "A", "B", "B", "A", "C"],
        "grp": ["X", "Y", "X", "Y", "X", "X"],
        "num": [10, 20, 30, 40, 10, 10],
    })
    df = df.group_by(['cat', 'grp']).agg(pl.col('num').first())

    path = marble(df, [1], [0])

    assert opened == [f"file://{path}"]

