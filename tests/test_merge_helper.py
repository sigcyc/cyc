from cyc.data_loaders import load_data
from datetime import date
import polars as pl
from cyc.merge_helper import MergeHelper
from cyc import Df


def test_join_asof():

    df = load_data("20241211-20241213", "polygon_test")

    mh = MergeHelper.merge(df["time"].dt.offset_by('1h'), df["time"], df["sym"], df["sym"])
    df.with_columns(mh.get(df["price"]).alias('spot_min60')).to_df().s(date='20241211').p(['price', 'spot_min60'])






