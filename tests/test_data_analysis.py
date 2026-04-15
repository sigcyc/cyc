import polars as pl
from cyc.data_analysis import accum_ratio
from cyc.data_loaders import load_data
from cyc.data_analysis import accum_ratiop

def test_accum_ratiop():
    df = load_data("stock_data_day", "20241211-20241214").collect()

    df = df.with_columns(
        pl.col("window_start").cast(pl.Datetime("ns")).dt.date().alias("date"),
        pl.col("close").cut([0, 100, 300, 1000]).alias("bkt_price"),
    )

    res = df.pivot(['date', 'bkt_price'], index=['date', 'bkt_price'], values='volume', aggregate_function='sum')


    accum_ratiop(df, 'bkt_price', 'date', 'volume')




def test_accum_ratio():
    df = pl.DataFrame({
        "cat": ["A", "A", "B", "B"],
        "grp": ["X", "Y", "X", "Y"],
        "num": [10, 20, 30, 40],
        "denom": [100, 200, 300, 400],
    })

    result = accum_ratio(df, "cat", "grp", "num", "denom")

    # Cell ratios: 10/100=0.1, 20/200=0.1, 30/300=0.1, 40/400=0.1
    assert result[0, "X"] == 0.1
    assert result[0, "Y"] == 0.1
    assert result[1, "X"] == 0.1
    assert result[1, "Y"] == 0.1

    # Row ratios: A=(10+20)/(100+200)=0.1, B=(30+40)/(300+400)=0.1
    assert result[0, "row_ratio"] == 0.1
    assert result[1, "row_ratio"] == 0.1

    # Row sums (denominator): A=300, B=700
    assert result[0, "row_sum"] == 300
    assert result[1, "row_sum"] == 700

    # Footer col_ratio row: X=(10+30)/(100+300)=0.1, Y=(20+40)/(200+400)=0.1
    assert result[2, "cat"] == "col_ratio"
    assert result[2, "X"] == 0.1
    assert result[2, "Y"] == 0.1

    # Footer col_sum row
    assert result[3, "cat"] == "col_sum"
    assert result[3, "X"] == 400
    assert result[3, "Y"] == 600


