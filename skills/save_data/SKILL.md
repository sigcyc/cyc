---
name: save_data
description: Convert a script into a save script, or batch save data across a date range using parallel processes. Use when the user asks to save data for a date range or to make a script batch-saveable.
argument-hint: <script_path> [<date_range>] [--processes N] [--data-dir path]
allowed-tools: Bash, Read, Edit
---

Two modes. Pick from the arguments: a date range means batch save; no date range means convert. Do not read any file other than the ones each mode names.

## Convert

Read only the target script. Rewrite it in place to this exact shape, moving its existing DataFrame-building logic into `main` so the final DataFrame is bound to `df`:

```python
import typer
import polars as pl
from pathlib import Path


def main(
    date: str = "20260326",
    data_dir: str = "",
    write: bool = False,
):
    df = ...  # existing logic ends with the final DataFrame in `df`

    if write:
        path = Path(data_dir) / f"{date}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
    else:
        globals().update(locals())


if __name__ == "__main__":
    typer.run(main)
```

Rules:
- Keep the script's other imports; add only what the shape above needs.
- The file must be named `save_<df_type>.py` — `batch_save.py` derives the df_type from the filename. Rename it if needed.
- `data_dir` is provided by `batch_save.py`; the script must not compute its own output path.

## Batch save

Run `${CLAUDE_SKILL_DIR}/scripts/batch_save.py` with the user's arguments. Always use `run_in_background: true` on the Bash tool call.

```bash
python ${CLAUDE_SKILL_DIR}/scripts/batch_save.py <script_path> <date_range>
```

- `<script_path>`: Relative path to the save script from the project root (e.g., `scripts/save_foo.py`)
- `<date_range>`: YYYYMMDD or YYYYMMDD-YYYYMMDD. Expanded to trading days via `cyc.util_time.parse_dates`
- `--processes`: Number of parallel processes (default 18)
- `--data-dir`: Optional override for the output directory (default: `get_data_path(df_type)` from the registry)

The script prints `[i/N] OK|FAIL <date>` per completed task (flushed). Use `BashOutput` on the background shell id to read incremental progress.

## Example

User: `/save_data scripts/save_foo.py 20250101-20250110`

```bash
python ${CLAUDE_SKILL_DIR}/scripts/batch_save.py scripts/save_foo.py 20250101-20250110
```
