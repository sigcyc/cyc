from .types import SymType
import polars as pl
from .config import get_data_path, get_calendar
from .time_util import parse_dates


def load_data_single(df_type: str) -> pl.LazyFrame:
    return pl.scan_parquet(get_data_path(df_type) / df_type, hive_partitioning=True)


def _get_date_list(date_str: str | pl.Series, df_type: str) -> list[str]:
    if isinstance(date_str, pl.Series):
        return [d.strftime("%Y%m%d") for d in date_str.to_list()]
    return parse_dates(date_str, get_calendar(df_type))


def load_data(date_str: str | pl.Series, df_type: str) -> pl.LazyFrame:
    date_list = _get_date_list(date_str, df_type)
    data_root = get_data_path(df_type) / df_type

    files, missing = [], []
    for d in date_list:
        f = data_root / f"{d}.parquet"
        if f.exists():
            files.append(f)
        else:
            missing.append(d)

    if missing:
        print("missing_dates: " + ", ".join(missing))
    if not files:
        raise FileNotFoundError(f"No data found in '{data_root}'")

    return pl.scan_parquet(files)


def load_data_hive_sym(date_str: str | pl.Series, df_type: str, sym: SymType = None) -> pl.LazyFrame:
    date_list = _get_date_list(date_str, df_type)
    data_root = get_data_path(df_type) / df_type

    if sym is None:
        syms = [p.name.split("=", 1)[1] for p in sorted(data_root.iterdir()) if p.is_dir() and p.name.startswith("sym=")]
    elif isinstance(sym, (str, int)):
        syms = [sym]
    else:
        syms = list(sym)

    files, missing = [], []
    for s in syms:
        for d in date_list:
            f = data_root / f"sym={s}" / f"{d}.parquet"
            if f.exists():
                files.append(f)
            else:
                missing.append(f"{s}/{d}")

    if missing:
        print("missing_dates: " + ", ".join(missing))
    if not files:
        raise FileNotFoundError(f"No data found in '{data_root}'")

    return pl.scan_parquet(files, hive_partitioning=True)
