from __future__ import annotations

from datetime import date as _date, datetime, timedelta
from functools import lru_cache

import polars as pl


def parse_time_to_ns(raw: str) -> int:
    """
    raw: 9:30 or 9:30:12.5
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("time string cannot be empty")

    parts = raw.split(":")
    if len(parts) > 3:
        raise ValueError(f"Invalid time string '{raw}'")

    sec_part = parts[2] if len(parts) == 3 else "0"
    if "." in sec_part:
        sec_str, frac_str = sec_part.split(".", 1)
    else:
        sec_str, frac_str = sec_part, ""

    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) >= 2 else 0
        second = int(sec_str) if sec_str else 0
    except ValueError as exc:
        raise ValueError(f"Invalid time string '{raw}'") from exc

    if not (0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60):
        raise ValueError(f"Invalid time string '{raw}'")

    if frac_str and not frac_str.isdigit():
        raise ValueError(f"Invalid time string '{raw}'")

    nanosecond = int(frac_str[:9].ljust(9, "0")) if frac_str else 0
    total_seconds = hour * 3600 + minute * 60 + second
    return total_seconds * 1_000_000_000 + nanosecond


def parse_dates(date: str) -> list[str]:
    """
    Given a date in the format of YYYYMMDD-YYYYMMDD. For example '20240101-20240110',
    return a list of dates that are trading days
    """
    raw = date.strip()
    if not raw:
        raise ValueError("date string cannot be empty")

    parts = [part.strip() for part in raw.split("-")]
    if len(parts) == 1:
        start_str = end_str = parts[0]
    elif len(parts) == 2 and parts[0] and parts[1]:
        start_str, end_str = parts
    else:
        raise ValueError(f"Invalid date range '{date}'")

    try:
        start = datetime.strptime(start_str, "%Y%m%d").date()
        end = datetime.strptime(end_str, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date range '{date}'") from exc

    if start > end:
        raise ValueError("start date must be before end date")

    trading_days: list[str] = []
    current = start
    while current <= end:
        if _is_trading_day(current):
            trading_days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return trading_days


def previous_trading_day(date: pl.Series) -> pl.Series:
    """Given date, calculate the previous trading day."""

    def _prev(d: _date) -> _date:
        d -= timedelta(days=1)
        while not _is_trading_day(d):
            d -= timedelta(days=1)
        return d

    return date.map_elements(_prev, return_dtype=pl.Date)


def next_trading_day(date: pl.Series) -> pl.Series:
    """Given date, calculate the next trading day."""

    def _next(d: _date) -> _date:
        d += timedelta(days=1)
        while not _is_trading_day(d):
            d += timedelta(days=1)
        return d

    return date.map_elements(_next, return_dtype=pl.Date)


def _is_trading_day(day: _date) -> bool:
    return day.weekday() < 5 and day not in _us_market_holidays(day.year)


@lru_cache(maxsize=None)
def _us_market_holidays(year: int) -> set[_date]:
    holidays: set[_date] = set()

    def add_observed(day: _date) -> None:
        observed = _apply_observed_rule(day)
        if observed.year == year:
            holidays.add(observed)

    holidays.add(_nth_weekday_of_month(year, 1, 0, 3))  # MLK Day
    holidays.add(_nth_weekday_of_month(year, 2, 0, 3))  # Presidents' Day
    holidays.add(_good_friday(year))
    holidays.add(_last_weekday_of_month(year, 5, 0))  # Memorial Day
    holidays.add(_nth_weekday_of_month(year, 9, 0, 1))  # Labor Day
    holidays.add(_nth_weekday_of_month(year, 11, 3, 4))  # Thanksgiving

    for month, day in ((1, 1), (6, 19), (7, 4), (12, 25)):
        add_observed(_date(year, month, day))

    next_new_year = _apply_observed_rule(_date(year + 1, 1, 1))
    if next_new_year.year == year:
        holidays.add(next_new_year)

    return holidays


def _apply_observed_rule(day: _date) -> _date:
    weekday = day.weekday()
    if weekday == 5:  # Saturday
        return day - timedelta(days=1)
    if weekday == 6:  # Sunday
        return day + timedelta(days=1)
    return day


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> _date:
    first = _date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    day = 1 + offset + (n - 1) * 7
    return _date(year, month, day)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> _date:
    if month == 12:
        next_month = _date(year + 1, 1, 1)
    else:
        next_month = _date(year, month + 1, 1)
    last = next_month - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _good_friday(year: int) -> _date:
    return _calculate_easter(year) - timedelta(days=2)


def _calculate_easter(year: int) -> _date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return _date(year, month, day)
