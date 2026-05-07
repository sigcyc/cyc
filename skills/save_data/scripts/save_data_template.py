import typer
import polars as pl
from pathlib import Path
from cyc import *

DF_TYPE = Path(__file__).stem.removeprefix("save_")


def main(
    date: str = "20260326",
    write: bool = False,
):
    df = pl.DataFrame()

    if write:
        path = get_data_path(DF_TYPE) / f"{date}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
    else:
        globals().update(locals())


if __name__ == "__main__":
    typer.run(main)
