from pathlib import Path

import polars as pl

from .config import get_data_path, get_calendar, get_file_layout
from .types import SymType
from .util_time import parse_dates


def _get_date_list(df_type: str, date_str: str | pl.Series) -> list[str]:
    if isinstance(date_str, pl.Series):
        return [d.strftime("%Y%m%d") for d in date_str.to_list() if d is not None]
    return parse_dates(date_str, get_calendar(df_type))


def _existing_files(data_root: Path, candidates: list[tuple[str, Path]]) -> list[Path]:
    """Keep candidates that exist on disk; report (label, path) misses by label."""
    files, missing = [], []
    for label, path in candidates:
        if path.exists():
            files.append(path)
        else:
            missing.append(label)
    if missing:
        print("missing_dates: " + ", ".join(missing))
    if not files:
        raise FileNotFoundError(f"No data found in '{data_root}'")
    return files


def load_data(df_type: str, date_str: str | pl.Series | None = None, sym: SymType = None) -> pl.LazyFrame:
    """
    single | single_hive_sym: date_str and sym are ignored
    date: sym is ignored
    """
    match get_file_layout(df_type):
        case "hive_sym":
            assert date_str is not None
            return load_data_hive_sym(df_type, date_str, sym)
        case "single" | "single_hive_sym":
            return load_data_single(df_type)
        case "date":
            assert date_str is not None
            return load_data_date(df_type, date_str)
        case _:
            raise ValueError("Unknown file_layout")


def load_data_single(df_type: str) -> pl.LazyFrame:
    return pl.scan_parquet(
        get_data_path(df_type) / "**/*.parquet",
        hive_partitioning=True,
        missing_columns="insert",
    )


def load_data_date(df_type: str, date_str: str | pl.Series) -> pl.LazyFrame:
    date_list = _get_date_list(df_type, date_str)
    data_root = get_data_path(df_type)
    files = _existing_files(data_root, [(d, data_root / f"{d}.parquet") for d in date_list])

    # The latest file decides the schema: backfill columns it has but older files lack
    # (missing_columns), and drop columns older files have but it lacks (extra_columns).
    return (
        pl.scan_parquet(
            files,
            schema=pl.scan_parquet(files[-1]).collect_schema(),
            missing_columns="insert",
            extra_columns="ignore",
            include_file_paths="__path__",
        )
        .with_columns(pl.col("__path__").str.extract(r"(\d{8})\.parquet$").str.to_date("%Y%m%d").alias("date"))
        .drop("__path__")
    )


def load_data_hive_sym(df_type: str, date_str: str | pl.Series, sym: SymType = None) -> pl.LazyFrame:
    date_list = _get_date_list(df_type, date_str)
    data_root = get_data_path(df_type)

    if sym is None:
        syms = [
            p.name.split("=", 1)[1] for p in sorted(data_root.iterdir()) if p.is_dir() and p.name.startswith("sym=")
        ]
    elif isinstance(sym, (str, int)):
        syms = [sym]
    else:
        syms = list(sym)

    files = _existing_files(
        data_root, [(f"{s}/{d}", data_root / f"sym={s}" / f"{d}.parquet") for s in syms for d in date_list]
    )
    return pl.scan_parquet(files, hive_partitioning=True, missing_columns="insert")
