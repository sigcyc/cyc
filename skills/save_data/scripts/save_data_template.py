import typer
import polars as pl
from pathlib import Path
from cyc.config import get_data_dir

NAME = Path(__file__).stem.removeprefix("save_")


def main(
    date: str = "20260326",
    data_dir: str | None = None,
    write: bool = False,
):
    df = pl.DataFrame()

    base = Path(data_dir or get_data_dir())
    path = base / NAME.replace("__", "/") / f"{date}.parquet"

    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
    else:
        globals().update(locals())


if __name__ == "__main__":
    typer.run(main)
