from __future__ import annotations

from datetime import date as Date, datetime, timedelta
from functools import lru_cache

import exchange_calendars as xcals
import polars as pl

_CALENDAR_CODES = {"nyse": "XNYS", "sse": "XSHG"}
# all_days has no exchange; it trades around the clock, so anchor it to America.
_CALENDAR_TIME_ZONES = {"nyse": "America/New_York", "sse": "Asia/Shanghai", "all_days": "America/New_York"}

_default_calendar = "nyse"


def set_default_calendar(calendar: str) -> None:
    """Session-wide calendar for date math outside any df_type. Set once per study.

    Code inside cyc/ must pass calendar explicitly (from the df_type it touches);
    only end-user scripts lean on this default.
    """
    global _default_calendar
    _default_calendar = calendar


@lru_cache(maxsize=None)
def _exchange_calendar(name: str):
    return xcals.get_calendar(_CALENDAR_CODES[name])


def calendar_time_zone(calendar: str | None = None) -> str:
    return _CALENDAR_TIME_ZONES[calendar or _default_calendar]


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


def parse_dates(date: str, calendar: str | None = None) -> list[str]:
    """
    Given a date in the format of YYYYMMDD-YYYYMMDD. For example '20240101-20240110',
    return a list of dates that are trading days
    """
    calendar = calendar or _default_calendar
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
        if _is_trading_day(current, calendar):
            trading_days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return trading_days


def _step_trading_day(day: Date, step: timedelta, calendar: str) -> Date:
    day += step
    while not _is_trading_day(day, calendar):
        day += step
    return day


def previous_trading_day(date: pl.Series, calendar: str | None = None) -> pl.Series:
    """Given date, calculate the previous trading day."""
    calendar = calendar or _default_calendar
    return date.map_elements(lambda d: _step_trading_day(d, timedelta(days=-1), calendar), return_dtype=pl.Date)


def next_trading_day(date: pl.Series, calendar: str | None = None) -> pl.Series:
    """Given date, calculate the next trading day."""
    calendar = calendar or _default_calendar
    return date.map_elements(lambda d: _step_trading_day(d, timedelta(days=1), calendar), return_dtype=pl.Date)


@lru_cache(maxsize=1024)
def _is_trading_day(day: Date, calendar: str) -> bool:
    if calendar == "all_days":
        return True
    return _exchange_calendar(calendar).is_session(day)


@lru_cache(maxsize=1024)
def _next_standard_expiration(date: Date, calendar: str) -> Date:
    year, month = date.year, date.month
    step = timedelta(days=-1 if calendar == "nyse" else 1)
    while True:
        first = Date(year, month, 1)
        d = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
        while not _is_trading_day(d, calendar):
            d += step
        # if third Friday is before date, go to next month
        if d >= date:
            return d
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)

def next_standard_expiration(date: pl.Series, calendar: str | None = None) -> pl.Series:
    calendar = calendar or _default_calendar
    mapping = {d: _next_standard_expiration(d, calendar) for d in date.unique()}
    return date.replace_strict(mapping, return_dtype=pl.Date)
