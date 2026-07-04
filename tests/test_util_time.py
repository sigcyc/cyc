import pytest

from cyc import util_time
from cyc.util_time import parse_time_to_ns, parse_dates, seconds_since_midnight, set_default_calendar


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


def test_seconds_since_midnight():
    # 2024-06-03 13:30:00 UTC == 09:30 America/New_York (EDT) == 21:30 Asia/Shanghai
    summer = 1_717_421_400 * 10**9
    assert seconds_since_midnight(summer) == 9 * 3600 + 30 * 60
    assert seconds_since_midnight(summer, "sse") == 21 * 3600 + 30 * 60

    # 2024-01-15 14:30:00 UTC == 09:30 America/New_York (EST)
    winter = 1_705_329_000 * 10**9
    assert seconds_since_midnight(winter) == 9 * 3600 + 30 * 60

    # nanoseconds survive as fractional seconds
    assert seconds_since_midnight(summer + 500_000_000) == 9 * 3600 + 30 * 60 + 0.5


def test_default_calendar_resolution(monkeypatch):
    # monkeypatch restores the session default after the test
    monkeypatch.setattr(util_time, "_default_calendar", "nyse")

    # initial default (nyse): golden week days are ordinary NYSE sessions
    assert parse_dates("20241001-20241008")[0] == "20241001"

    # session default: SSE is closed for golden week, first session is Oct 8
    set_default_calendar("sse")
    assert parse_dates("20241001-20241008") == ["20241008"]

    # explicit argument beats the session default
    assert parse_dates("20241001-20241003", "nyse") == ["20241001", "20241002", "20241003"]


def test_parse_date_rejects_invalid_ranges():
    with pytest.raises(ValueError):
        parse_dates("20240105-20240101")

    with pytest.raises(ValueError):
        parse_dates("bad-input")
