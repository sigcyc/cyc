import pytest

from cyc.time_util import parse_time_to_ns, parse_dates


def _ns(hours: int, minutes: int, seconds: int, nanos: int = 0) -> int:
    """Helper to convert to nanoseconds for clearer expectations."""
    return ((hours * 3600) + (minutes * 60) + seconds) * 1_000_000_000 + nanos


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("9:30", _ns(9, 30, 0)),
        ("23:59:59", _ns(23, 59, 59)),
        ("00:00:00.000000001", 1),
        ("12:05:07.123", _ns(12, 5, 7, 123_000_000)),
        ("01:02:03.1234567899", _ns(1, 2, 3, 123_456_789)),
        (" 3:04:05.6 ", _ns(3, 4, 5, 600_000_000)),
    ],
)
def test_parse_time_to_ns_valid_inputs(raw, expected):
    assert parse_time_to_ns(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "  ",
        "09-30",
        "24:00",
        "12:60",
        "12:30:aa",
        "12:30:00.x",
    ],
)
def test_parse_time_to_ns_invalid_inputs(raw):
    with pytest.raises(ValueError):
        parse_time_to_ns(raw)


def test_parse_date_skips_weekends():
    assert parse_dates("20241211-20241216") == [
        "20241211",
        "20241212",
        "20241213",
        "20241216",
    ]


def test_parse_date_skips_holidays():
    # Christmas Day 2023 is a Monday and should be excluded
    assert parse_dates("20231222-20231227") == [
        "20231222",
        "20231226",
        "20231227",
    ]


def test_parse_date_rejects_invalid_ranges():
    with pytest.raises(ValueError):
        parse_dates("20240105-20240101")

    with pytest.raises(ValueError):
        parse_dates("bad-input")
