import polars as pl
import cyc.data_frame_monkey_patch  # noqa: F401 - registers the cyc expression namespace
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

    result = accum_ratio(df, "cat", "grp", "num", "denom").df

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


def test_accum_ratio_accepts_expr_values_and_sorts():
    df = pl.DataFrame({
        "cat": ["B", "A", "B", "A"],
        "grp": ["Y", "Y", "X", "X"],
        "num": [40, 20, 30, 10],
        "denom": [400, 200, 300, 100],
    })

    result = accum_ratio(df, "cat", "grp", pl.col("num") * 2, pl.col("denom")).df

    assert result.columns == ["cat", "X", "Y", "row_ratio", "row_sum"]
    assert result["cat"].to_list() == ["A", "B", "col_ratio", "col_sum"]
    assert result[0, "X"] == 0.2
    assert result[1, "Y"] == 0.2
    assert result[2, "row_ratio"] == 0.2


def test_accum_ratio_filters_before_ratio():
    df = pl.DataFrame({
        "cat": ["A", "A"],
        "grp": ["X", "X"],
        "num": [10, 90],
        "denom": [100, 900],
        "keep": [True, False],
    })

    result = accum_ratio(df, "cat", "grp", "num", "denom", filter=pl.col("keep")).df

    assert result[0, "X"] == 0.1
    assert result[0, "row_sum"] == 100


def test_accum_ratio_handles_zero_column_denominator():
    df = pl.DataFrame({
        "cat": ["A", "A"],
        "grp": ["X", "Y"],
        "num": [10, 20],
        "denom": [0, 100],
    })

    result = accum_ratio(df, "cat", "grp", "num", "denom").df
    col_ratio = result.filter(pl.col("cat") == "col_ratio")

    assert col_ratio[0, "X"] is None
    assert col_ratio[0, "Y"] == 0.2


def test_accum_ratio_sorts_cut_rows_and_columns():
    df = pl.DataFrame({
        "grp": ["B", "A", "A", "B", "B", "B"],
        "price": [250.0, 50.0, 150.0, 500.0, 5.0, -1.0],
        "num": [1, 2, 3, 4, 5, 6],
        "denom": [10, 20, 30, 40, 50, 60],
    }).with_columns(pl.col("price").cyc.cut([0, 100, 300], f=pl.col("price") > 0))

    row_result = accum_ratio(df, "price_cut", "grp", "num", "denom").df

    assert row_result.columns == ["price_cut", "A", "B", "row_ratio", "row_sum"]
    assert row_result["price_cut"].to_list() == [
        "(0, 100]",
        "(100, 300]",
        "(300, inf]",
        "filtered",
        "col_ratio",
        "col_sum",
    ]

    column_result = accum_ratio(df, "grp", "price_cut", "num", "denom").df

    assert column_result.columns == [
        "grp",
        "(0, 100]",
        "(100, 300]",
        "(300, inf]",
        "filtered",
        "row_ratio",
        "row_sum",
    ]
    assert column_result["grp"].to_list() == ["A", "B", "col_ratio", "col_sum"]


def test_accum_ratio_filter_maps_cut_cells_back():
    df = pl.DataFrame({
        "grp": ["B", "A", "A", "B", "B", "B"],
        "price": [250.0, 50.0, 150.0, 500.0, 5.0, -1.0],
        "num": [1, 2, 3, 4, 5, 6],
        "denom": [10, 20, 30, 40, 50, 60],
    }).with_columns(pl.col("price").cyc.cut([0, 100, 300], f=pl.col("price") > 0))

    result = accum_ratio(df, "price_cut", "grp", "num", "denom")

    # rows: ["(0, 100]", "(100, 300]", "(300, inf]", "filtered"]; columns: ["A", "B"]
    assert result.filter(df, 0, 0)["num"].to_list() == [2]      # (0, 100] & A
    assert sorted(result.filter(df, None, 0)["num"].to_list()) == [2, 3]  # all of A
    assert result.filter(df, 3, 1)["num"].to_list() == [6]      # filtered & B
