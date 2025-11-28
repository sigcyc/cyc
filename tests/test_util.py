import pytest

from cyc.util import parse_time_to_ns


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
        "9",
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
