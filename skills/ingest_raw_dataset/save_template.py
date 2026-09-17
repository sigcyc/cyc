"""Template for DATASET/save_<DATASET>.py — copy into the dataset dir and fill the <...>.

Used by skills/ingest_raw_dataset. Same signature as the save_data skill's scripts,
so batch_save.py drives it: one date per run, output dir passed in. Reads the raw
file for that date, renames columns per columns.yaml, casts the required schema,
and writes one normalized parquet. Do NOT re-derive the cyc API — none is needed.
"""
from pathlib import Path

import polars as pl
import typer
import yaml

RAW_SRC = Path("<DATASET or DATASET_raw>")  # never written to
_cols = Path(__file__).parent / "columns.yaml"  # optional: absent when no renames
RENAMES = (yaml.safe_load(_cols.read_text()) or {}) if _cols.exists() else {}


def raw_file(date: str) -> Path:
    # date is YYYYMMDD. Map it to the raw file, e.g. RAW_SRC / f"{date}.csv" or
    # RAW_SRC / f"{date[:4]}-{date[4:6]}-{date[6:]}.csv".
    # single / single_hive_sym: the raw is one file -> return RAW_SRC (date unused).
    return RAW_SRC / "<file for date>"


def main(
    date: str = "<YYYYMMDD of one raw file>",
    data_dir: str = "",
    write: bool = False,
):
    df = pl.read_csv(raw_file(date))  # or read_parquet / read_ipc to match the raw format
    df = df.rename({k: v for k, v in RENAMES.items() if k in df.columns})
    # Required output schema. Keep ALL other columns unchanged — never drop.
    df = df.with_columns(
        pl.col("sym").cast(pl.String),          # or pl.UInt64
        pl.col("time").cast(pl.Datetime("ns")),  # and/or pl.col("date").cast(pl.Date)
    )

    if write:
        # Pick ONE to match file_layout (see data_pipeline.md, symlinked in this dir):
        path = Path(data_dir) / f"{date}.parquet"                       # date (default)
        # path = Path(data_dir) / f"sym={'<sym>'}" / f"{date}.parquet"  # hive_sym
        # path = Path(data_dir) / "part0.parquet"                       # single
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
    else:
        globals().update(locals())


if __name__ == "__main__":
    typer.run(main)
