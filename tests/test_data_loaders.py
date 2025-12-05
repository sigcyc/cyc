import pytest

from cyc.data_loaders import parse_date


def test_parse_date_skips_weekends():
    assert parse_date("20241211-20241216") == [
        "20241211",
        "20241212",
        "20241213",
        "20241216",
    ]


def test_parse_date_skips_holidays():
    # Christmas Day 2023 is a Monday and should be excluded
    assert parse_date("20231222-20231227") == [
        "20231222",
        "20231226",
        "20231227",
    ]


def test_parse_date_rejects_invalid_ranges():
    with pytest.raises(ValueError):
        parse_date("20240105-20240101")

    with pytest.raises(ValueError):
        parse_date("bad-input")
