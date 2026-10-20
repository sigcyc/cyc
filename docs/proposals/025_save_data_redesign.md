# Proposal: save_data redesign

Goal: `/save_data <df_type> <range>` saves any dataset. I never need to remember where its script lives or how many steps it takes.

Context:
- Pain points after using save_data for a while:
  1. I need to remember the folder name.
  2. Some saves have multiple steps, not a single python file.
- Today `batch_save.py <script> <range>` takes a script path, derives df_type from the filename `save_<df_type>.py`, expands the range with the df_type's calendar, and runs the script once per date in parallel.
- Scripts that don't fit today: form13f, us_stock_ticks and us_stock_ratios run their own loop; us_stock_financials and futures_report take no date; a_stock_* is an external binary driven by an agent spec.
- cn_futures_d1 showed a multi-step save (download monthly zip → unzip → convert) collapses into one per-date script (`data_scripts/cn_futures/save_cn_futures_d1.py`) once the steps are fixed commands.
- Industry practice (Dagster partitions, Airflow data intervals, Luigi `RangeDaily`): the orchestrator owns the loop, the dataset declares its partitions, the code handles one partition per run. Behavior never depends on file type.

Design:
1. **The registry says where the script is.** Each `df_types.yaml` entry gets a `script:` key, relative to the folder holding the `cyc` clone (repos are cloned side by side), e.g. `script: data_scripts/cn_futures/save_cn_futures_d1.py`. Absolute and `~` paths also work: joining onto an absolute path returns it unchanged.
2. **One loop, in batch_save.** Every save script is a `.py` with `main(date, data_dir, write)` that handles one partition per run. No script loops. No `.md` script kind:
   - A fixed external command (the a_stock parser) gets a thin `.py` wrapper for one date.
   - A truly manual step (login, vendor upload) goes in a runbook that the script's error message points to. `/save_data` doesn't run it.
3. **The partition comes from `calendar`.** A calendar is the list of valid partition keys.
   - `nyse`, `sse`, `all_days`: unchanged.
   - `quarter_end` (new): the last calendar day of Mar/Jun/Sep/Dec, weekends included. form13f's `period_of_report` is the calendar quarter end, e.g. Sun 20240331, so a trading-day calendar would miss it.
   - `file_layout: single` or `single_hive_sym`: no date dimension, no partition. batch_save runs the script once and the range may be omitted.
   - Naming rule: name a calendar after the rule that produces its dates, not how often they occur. Future expiration calendars are `<exchange>_quarterly_expiration` (e.g. `cme_quarterly_expiration`, `cffex_quarterly_expiration`), because the holiday adjustment differs: US moves to the previous trading day, CFFEX to the next.
4. **One resolver, in batch_save:** `/save_data <df_type|path> [range]`. If the argument is a file, use it and take df_type from its filename; otherwise read the df_type's `script:` key. A missing `script:` key raises KeyError; no search fallback. Running by filename from inside a folder falls out of this.
5. **Skills split by lifecycle, not mode:**
   - `save_data`: run only.
   - `convert_save_script`: today's Convert mode, which also adds the `script:` key.
   - `ingest_raw_dataset`: stays separate.

Naming conventions:
- A dataset name ends in the bar length, always 1 or more: `us_stock_min1`, `us_stock_min5`, `us_stock_d1`.
- A column name ends in an offset from the row's time, any sign: `d0`, `d1`, `dm1`, `minm5`.
- Units: `min` minute, `h` hour, `d` day, `w` week, `m` month. `m` after the unit means minus: `mm1` is one month back.

Requirements:
- `cyc/config.py`: `get_script_path(df_type)` next to `get_data_path`, resolving `script:` against `_WORKSPACE = _DF_TYPES_PATH.parents[3]  # repos are cloned side by side`. batch_save only calls getters.
- `cyc/util_time.py`: add the `quarter_end` calendar to `_is_trading_day`, and a time zone for it in `_CALENDAR_TIME_ZONES` (America/New_York; every load looks one up).
- `skills/save_data/scripts/batch_save.py`:
  - Accept a df_type or a script path (Design 4).
  - For `single` and `single_hive_sym` layouts, run the script once without `--date`; the range is optional.
- `skills/save_data/SKILL.md`: run mode only, with the new argument line.
- New skill `convert_save_script`: the Convert mode moved out of save_data, plus adding the `script:` key to `df_types.yaml`.
- `skills/ingest_raw_dataset/SKILL.md`: step 4 adds `script:`; steps 6–7 call `/save_data DATASET <date>`; delete the step 5 instruction to run `single` scripts outside batch_save.

Migration (one dataset at a time, each independent):
- `us_stock_d1`, `sofr`: add `script:`; they already handle one date per run.
- `form13f`: one quarter per run, one file per quarter; registry gets `calendar: quarter_end` and drops `file_layout: single`.
- `us_stock_ticks`, `us_stock_ratios`: remove the internal loop, take `--date`.
- `cn_index_d1`: convert `data_scripts/cn_index/download_ic500.py` to the template shape (`single`, so no date).
- `us_stock_financials`: runs once, no date.
- `futures_report`: add a CLI; it runs once, no date.
- `a_stock_*`: per-date `.py` wrapper around the parser binary.
- `us_stock_trades`, `us_stock_quotes`: already one date per run, but need registry entries first.

Not now (add when a second real case appears):
- Range-based sources: Dagster's `BackfillPolicy.single_run()` hands one run a whole range. cn_futures turned out to have a per-day file, so nothing needs it yet.
- Dependencies: `cn_futures_d1__index` needs `cn_index_d1` refreshed first. If a second case appears, add a `deps:` key and have batch_save run upstream first.

Open question:
- Scripts that write several df_types (us_stock_financials writes 3, a_stock writes 4). The entries would share one `script:`, but `--data-dir` can't name several folders, so those scripts would look up their own outputs with `get_data_path`.

Verification:
- `parse_dates("20231201-20240701", "quarter_end") == ["20231231", "20240331", "20240630"]`.
- From `~`, `/save_data us_stock_min1 20260326` writes `/Volumes/ssd/us_stock_min1/20260326.parquet`.
- `/save_data cn_futures_d1 20261016` rewrites `/Volumes/ssd/cn_futures_d1/20261016.parquet`.
- From `~/workspace/data_scripts/massive_data`, `/save_data save_us_stock_min1.py 20260326` does the same as the df_type form.
- After the form13f migration, `/save_data form13f 20250101-20251231` runs 4 times: 20250331, 20250630, 20250930, 20251231.
