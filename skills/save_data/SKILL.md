---
name: save_data
description: Batch save a df_type across a date range using parallel processes. Use when the user asks to save data for a dataset and date range.
argument-hint: <df_type|script_path> [<date_range>] [--processes N] [--data-dir path]
allowed-tools: Bash
---

Run `${CLAUDE_SKILL_DIR}/scripts/batch_save.py` with the user's arguments. Always use `run_in_background: true` on the Bash tool call. Do not read any file.

```bash
python ${CLAUDE_SKILL_DIR}/scripts/batch_save.py <df_type|script_path> [<date_range>]
```

- `<df_type|script_path>`: a df_type (e.g. `us_stock_min1`), whose save script is its `script:` key in `df_types.yaml`; or a path to a `save_<df_type>.py` script.
- `<date_range>`: YYYYMMDD or YYYYMMDD-YYYYMMDD, expanded with the df_type's calendar via `cyc.util_time.parse_dates`. Omit it for `single` / `single_hive_sym` layouts, which run once.
- `--processes`: Number of parallel processes (default 18)
- `--data-dir`: Optional override for the output directory (default: `get_data_path(df_type)` from the registry)

The script prints `[i/N] OK|FAIL <date>` per completed task (flushed). Use `BashOutput` on the background shell id to read incremental progress.

To turn an existing script into a save script, use the `convert_save_script` skill.

## Example

User: `/save_data us_stock_min1 20250101-20250110`

```bash
python ${CLAUDE_SKILL_DIR}/scripts/batch_save.py us_stock_min1 20250101-20250110
```
