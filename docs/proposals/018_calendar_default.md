# Change: calendar default resolution in util_time

Status: done

## Problem

Every public function in `cyc/util_time.py` takes `calendar: str = "nyse"`. Users doing non-NYSE work must thread `calendar="sse"` through every call, even though a study works against one market for its whole life. The goal is zero mental load in the common case without hiding the calendar where it is a property of the data.

## Findings

1. Two calendar architectures already coexist. The data path is already right: calendar is a property of the df_type, declared in `df_types.yaml`, resolved by `config.get_calendar(df_type)` inside `load_data`, `Df._enrich`, and `batch_save`. Users never pass it. The pain is only in the free date-math functions (`parse_dates`, `next_trading_day`, `previous_trading_day`, `next_standard_expiration`).
2. "One calendar per project" cannot mean the repo: `df_types.yaml` uses nyse, sse, and all_days simultaneously. It means one calendar per session/script. So the ambient default must be process-scoped, not repo-scoped and not call-scoped.
3. The default "nyse" is defined four times: `df_types.yaml` `default.calendar`, the hard fallback in `config.py:22`, four public signatures, and two private cached functions in `util_time.py`. Four sources of truth for one fact.
4. `data_finance._get_spot` calls `next_trading_day(date)` bare. It is correct only by accident (`stock_data_day` happens to be nyse). Under any ambient-default scheme this becomes a real bug: a session set to sse would silently corrupt spot adjustment.

## Design

Two independent resolutions, one per scope:

```
date math:   explicit argument > session default (util_time._default_calendar, initially "nyse")
df_types:    yaml entry > yaml default.calendar
```

The session default and the catalog default are two facts, not one. The catalog default says what calendar un-annotated df_types have — a fact about data on disk. The session default says what market the current study works on — a fact about the user. They share the value "nyse" coincidentally, so neither derives from the other: deriving the session default from the yaml would couple free date math to the data catalog file and add a file read, a None sentinel state, and a util_time → config import to unify two values that are different facts.

Anything keyed by a df_type gets that df_type's calendar from yaml. The session default never leaks into df_type resolution — the data's calendar is a fact about the data.

## Changes

1. `cyc/util_time.py` owns the session default: module variable `_default_calendar = "nyse"` plus `set_default_calendar(name)`, exported from `cyc`. Public functions take `calendar: str | None = None` and resolve `calendar or _default_calendar` once at the boundary. Private functions (`_is_trading_day`, `_next_standard_expiration`, `_step_trading_day`): calendar is a required parameter with no default. Uniform rule — public resolves once at the boundary, private takes a concrete string. `lru_cache` keys stay concrete, so changing the session default mid-process cannot serve stale entries.
2. `cyc/config.py`: delete the hard `"nyse"` fallback in `get_calendar` — the catalog fallback is yaml `default.calendar`; if that line is missing, fail loud. No session state in config.
3. `cyc/data_finance.py`: `_get_spot` passes `get_calendar("stock_data_day")` explicitly. House rule: code inside `cyc/` always passes calendar explicitly (derived from the df_type it touches); only end-user scripts lean on the ambient default.
4. `cyc/types.py`: fix the stale comment `calendar: str  # calendar for time zone` — calendar also drives trading-day logic in the loading path.

User experience: an A-share script starts with `set_default_calendar("sse")` and every date-math call in that process follows. A US script writes nothing. Mixed-market data loading keeps working because it never consults the session default.

## Rejected alternatives

1. Status quo (per-signature default): the default does not remove the decision, it hides it. Every bare call is an unreviewed nyse assumption, and sse sessions thread `calendar=` everywhere.
2. Calendar object (QuantLib style) or a bound-functions factory: threads an object instead of a string — same load, more ceremony.
3. Context manager / contextvars (`with calendar("sse"):`): async-safe scoped override is the upgrade path if the "never changes mid-session" assumption ever breaks. Building it now solves a hypothetical.
4. Yaml-only (edit `default.calendar` per study): repo state would depend on which study ran last; the setter scopes the choice to the process, where it belongs.
5. Session default falling back to yaml `default.calendar` (first implementation, removed): saved one "nyse" literal by coupling session scope to the data catalog. The literal was not duplication — two scopes, two facts — and the coupling cost a getter function, a None state, file I/O per bare call, and a cross-module import.

Precedent for the pattern: `decimal.getcontext()`, `random`'s module-level instance, `pd.set_option`, matplotlib rcParams, `pl.Config`. Research-ergonomics tools converge on ambient config with an explicit escape hatch; fully-explicit calendars (exchange_calendars, QuantLib) are for libraries shipped to strangers and concurrent servers.
