from cyc.data_loaders import load_data
from datetime import date
import polars as pl
from cyc.merge_helper import MergeHelper
from cyc import Df


def test_join_asof():

    df = load_data("polygon_test", "20241211-20241213").collect()

    mh = MergeHelper.merge(df["time"].dt.offset_by('1h'), df["time"], df["sym"], df["sym"])
    Df(df.with_columns(mh.get(df["price"]).alias('spot_min60')), "polygon_test").s(date='20241211').p(['price', 'spot_min60'])






