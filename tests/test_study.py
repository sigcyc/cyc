from typing import cast

from cyc.df import Df


def test_add_stock_single_field():
    df = Df.load_data("stock_data_day", "20210715-20210721")
    nvda = df.s("NVDA").select("sym", "date")
    result = nvda.add_stock("close")
    assert "close" in result.columns
    assert len(result.df) == len(nvda.df)
    assert result["close"].null_count() == 0


def test_add_stock_multiple_fields():
    df = Df.load_data("stock_data_day", "20210715-20210721")
    nvda = df.s("NVDA").select("sym", "date")
    result = nvda.add_stock(["open", "close"])
    assert "open" in result.columns
    assert "close" in result.columns
    assert len(result.df) == len(nvda.df)


def test_add_spot_current_day():
    df = Df.load_data("stock_data_day", "20210715-20210721")
    nvda = df.s("NVDA")
    expected_close = nvda["close"]
    result = nvda.select("sym", "date").add_spot(0)
    assert "spot_d0" in result.columns
    max_diff = cast(float, (result["spot_d0"] - expected_close).abs().max())
    assert max_diff < 0.01


def test_add_spot_forward():
    df = Df.load_data("stock_data_day", "20210715-20210721")
    nvda = df.s("NVDA").select("sym", "date")
    result = nvda.add_spot(1)
    assert "spot_d1" in result.columns
    current = nvda.add_stock("close")
    assert ((result["spot_d1"] - current["close"]).abs() <= 100).all()


def test_add_spot_backward():
    df = Df.load_data("stock_data_day", "20210715-20210721")
    nvda = df.s("NVDA").select("sym", "date")
    result = nvda.add_spot(-1)
    assert "spot_dm1" in result.columns
    current = nvda.add_stock("close")
    assert ((result["spot_dm1"] - current["close"]).abs() <= 100).all()
