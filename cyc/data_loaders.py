from datetime import datetime
from pathlib import Path

import polars as pl

from .config import get_data_path, get_calendar, get_df_type_dict, get_file_layout
from .types import SymType
from .util_time import parse_dates


def _get_date_list(df_type: str, date_str: str | pl.Series) -> list[str]:
    if isinstance(date_str, pl.Series):
        return [d.strftime("%Y%m%d") for d in date_str.to_list() if d is not None]
    return parse_dates(date_str, get_calendar(df_type))


def _existing_groups(data_root: Path, candidates: list[tuple[str, list[Path]]]) -> list[tuple[str, list[Path]]]:
    """Keep candidates that have files on disk; report misses by label."""
    missing = [label for label, files in candidates if not files]
    if missing:
        print("missing_dates: " + ", ".join(missing))
    groups = [(label, files) for label, files in candidates if files]
    if not groups:
        raise FileNotFoundError(f"No data found in '{data_root}'")
    return groups


def _existing_files(data_root: Path, candidates: list[tuple[str, Path]]) -> list[Path]:
    """Keep candidates that exist on disk; report (label, path) misses by label."""
    groups = _existing_groups(data_root, [(label, [path] if path.exists() else []) for label, path in candidates])
    return [file for _, files in groups for file in files]


def _date_files(data_root: Path, date: str) -> list[Path]:
    """A date is either a flat `{date}.parquet` file or a `date={date}/` dir of parquet parts."""
    file = data_root / f"{date}.parquet"
    return [file] if file.exists() else sorted((data_root / f"date={date}").glob("*.parquet"))


def load_data(df_type: str, date_str: str | pl.Series | None = None, sym: SymType = None) -> pl.LazyFrame:
    """
    single | single_hive_sym: sym is ignored; date_str filters rows on the yaml date column (plain range, no calendar)
    date: sym is ignored
    """
    match get_file_layout(df_type):
        case "hive_sym":
            assert date_str is not None
            return load_data_hive_sym(df_type, date_str, sym)
        case "single" | "single_hive_sym":
            return load_data_single(df_type, date_str)
        case "date":
            assert date_str is not None
            return load_data_date(df_type, date_str)
        case _:
            raise ValueError("Unknown file_layout")


def load_data_single(df_type: str, date_str: str | pl.Series | None = None) -> pl.LazyFrame:
    lf = pl.scan_parquet(
        get_data_path(df_type) / "**/*.parquet",
        hive_partitioning=True,
        missing_columns="insert",
    )
    if date_str is None:
        return lf
    date_col = pl.col(get_df_type_dict(df_type).get('date', 'date'))
    if isinstance(date_str, pl.Series):
        return lf.filter(date_col.is_in(date_str.implode()))
    start_str, _, end_str = date_str.partition("-")
    start = datetime.strptime(start_str, "%Y%m%d").date()
    end = datetime.strptime(end_str, "%Y%m%d").date() if end_str else start
    return lf.filter(date_col.is_between(start, end))


def load_data_date(df_type: str, date_str: str | pl.Series) -> pl.LazyFrame:
    date_list = _get_date_list(df_type, date_str)
    data_root = get_data_path(df_type)
    groups = _existing_groups(data_root, [(d, _date_files(data_root, d)) for d in date_list])

    # The latest file decides the schema: backfill columns it has but older files lack
    # (missing_columns), and drop columns older files have but it lacks (extra_columns).
    schema = pl.scan_parquet(groups[-1][1][-1]).collect_schema()  # groups[-1] is the latest (date, files); [1][-1] its last file
    return pl.concat(
        pl.scan_parquet(files, schema=schema, missing_columns="insert", extra_columns="ignore").with_columns(
            pl.lit(date).str.to_date("%Y%m%d").alias("date")
        )
        for date, files in groups
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
