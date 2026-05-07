from .types import SymType
import polars as pl
from .config import get_data_path, get_calendar, get_file_layout
from .time_util import parse_dates


def _get_date_list(df_type: str, date_str: str | pl.Series) -> list[str]:
    if isinstance(date_str, pl.Series):
        return [d.strftime("%Y%m%d") for d in date_str.to_list()]
    return parse_dates(date_str, get_calendar(df_type))


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

    return pl.scan_parquet(files, missing_columns="insert")


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

    return pl.scan_parquet(files, hive_partitioning=True, missing_columns="insert")
