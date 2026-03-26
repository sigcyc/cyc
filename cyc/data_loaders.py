import polars as pl
from datetime import datetime
from tqdm import tqdm
from .types import get_data_path
from .time_util import parse_dates


def load_data_single(df_type: str) -> pl.DataFrame:
    return pl.read_parquet(get_data_path(df_type) / f"{df_type}.parquet")


def load_data(date_str: str | pl.Series, df_type: str) -> pl.DataFrame:
    if isinstance(date_str, pl.Series):
        date_list = [d.strftime("%Y%m%d") for d in date_str.to_list()]
    else:
        date_list = parse_dates(date_str)
    data_root = get_data_path(df_type) / df_type
    if not data_root.exists():
        raise FileNotFoundError(f"Data path '{data_root}' does not exist")

    if not date_list:
        raise ValueError(f"No dates provided or found in range")

    frames: list[pl.DataFrame] = []
    missing_dates: list[str] = []

    for date in tqdm(date_list):
        file_path = data_root / f"{date}.parquet"
        if not file_path.exists():
            missing_dates.append(date)
            continue
        date_value = datetime.strptime(date, "%Y%m%d").date()
        frames.append(
            pl.read_parquet(file_path).with_columns(pl.lit(date_value).alias("date"))
        )

    if missing_dates:
        print("missing_dates:" + ", ".join(missing_dates))

    combined = pl.concat(frames, how="vertical_relaxed", rechunk=True)
    return combined
