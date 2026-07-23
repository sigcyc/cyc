import typer
import polars as pl
from pathlib import Path


def main(
    date: str = "20260326",
    data_dir: str = "",
    write: bool = False,
):
    df = pl.DataFrame()

    if write:
        path = Path(data_dir) / f"{date}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
    else:
        globals().update(locals())


if __name__ == "__main__":
    typer.run(main)
