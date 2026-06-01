"""Template for DATASET/convert.py — copy into the dataset dir and fill the <...>.

Used by skills/ingest_raw_dataset. Reads raw files, renames columns per
columns.yaml, casts the required schema, and writes normalized parquet rooted at
cyc.config.get_data_path(DATASET). Do NOT re-derive the cyc API — it is all here.
"""
import sys
from pathlib import Path

import polars as pl
import yaml

from cyc.config import get_data_path

DATASET = "<DATASET>"
RAW_SRC = Path("<DATASET or DATASET_raw>")  # never written to
OUT = get_data_path(DATASET)                # output root, from df_types.yaml data.path
_cols = Path(__file__).parent / "columns.yaml"  # optional: absent when no renames
RENAMES = (yaml.safe_load(_cols.read_text()) or {}) if _cols.exists() else {}


def convert_one(raw_file: Path) -> pl.DataFrame:
    df = pl.read_csv(raw_file)  # or read_parquet / read_ipc to match the raw format
    df = df.rename({k: v for k, v in RENAMES.items() if k in df.columns})
    # Required output schema. Keep ALL other columns unchanged — never drop.
    return df.with_columns(
        pl.col("sym").cast(pl.String),          # or pl.UInt64
        pl.col("time").cast(pl.Datetime("ns")),  # and/or pl.col("date").cast(pl.Date)
    )


def out_path(raw_file: Path) -> Path:
    yyyymmdd = "<derive from raw_file name or a column>"
    # Pick ONE to match file_layout (see data_pipeline.md, symlinked in this dir):
    return OUT / f"{yyyymmdd}.parquet"                       # date (default)
    # return OUT / f"sym={'<sym>'}" / f"{yyyymmdd}.parquet"  # hive_sym
    # return OUT / "part0.parquet"                           # single


def write(raw_file: Path) -> None:
    p = out_path(raw_file)
    p.parent.mkdir(parents=True, exist_ok=True)
    convert_one(raw_file).write_parquet(p)
    print(f"wrote {p}")


if __name__ == "__main__":
    # Dry-run (step 6): pass one file.  Full run (step 7): pass none -> all files.
    files = [Path(a) for a in sys.argv[1:]] or sorted(RAW_SRC.glob("*.csv"))
    for f in files:
        write(f)
