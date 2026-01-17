import polars as pl
from cyc.data_loaders import load_data
from cyc.data_analysis import accum_ratiop

def test_accum_ratio():
    df = load_data("20241211-20241214", "stock_data_day")

    df = df.with_columns(pl.col("close").cut([0, 100, 300, 1000]).alias("bkt_price"))

    res = df.pivot(['date', 'bkt_price'], index=['date', 'bkt_price'], values='volume', aggregate_function='sum')


    accum_ratiop(df, 'bkt_price', 'date', 'volume')

    import pdb; pdb.set_trace()


