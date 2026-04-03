import polars as pl
from tqdm import tqdm
from .config import get_data_path, get_calendar
from .time_util import parse_dates


def load_data_single(df_type: str) -> pl.LazyFrame:
    return pl.scan_parquet(get_data_path(df_type) / f"{df_type}.parquet")


def load_data(date_str: str | pl.Series, df_type: str) -> pl.LazyFrame:
    if isinstance(date_str, pl.Series):
        date_list = [d.strftime("%Y%m%d") for d in date_str.to_list()]
    else:
        date_list = parse_dates(date_str, get_calendar(df_type))
    data_root = get_data_path(df_type) / df_type

    files, missing = [], []
    for d in tqdm(date_list):
        found = list(data_root.glob(f"**/{d}.parquet"))
        if found:
            files.extend(found)
        else:
            missing.append(d)

    if missing:
        print("missing_dates: " + ", ".join(missing))
    if not files:
        raise FileNotFoundError(f"No data found in '{data_root}'")

    return pl.scan_parquet(files, hive_partitioning=True)
