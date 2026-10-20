---
name: convert_save_script
description: Rewrite a script into a one-date-per-run save script and register it in df_types.yaml, so /save_data <df_type> can run it. Use when the user asks to make a script batch-saveable.
argument-hint: <script_path>
allowed-tools: Bash, Read, Edit
---

Do not read any file other than the target script and `df_types.yaml`.

1. Read only the target script. Rewrite it in place to this exact shape, moving its existing DataFrame-building logic into `main` so the final DataFrame is bound to `df`:

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

   - Keep the script's other imports; add only what the shape above needs.
   - The file must be named `save_<df_type>.py`. Rename it if needed.
   - `data_dir` is provided by `batch_save.py`; the script must not compute its own output path.
   - Handle exactly one date per run. If the script loops over dates, remove the loop; `/save_data` owns it.

2. Register it. Print the registry path and the workspace root:

```bash
python -c "from cyc.config import _DF_TYPES_PATH, _WORKSPACE; print(_DF_TYPES_PATH, _WORKSPACE)"
```

   Add `script: <path>` to the df_type's entry in that `df_types.yaml`: relative to the workspace root if the script is inside it (e.g. `data_scripts/massive_data/save_us_stock_min1.py`), else absolute. If the df_type has no entry, tell the user; registering a new dataset is `ingest_raw_dataset`'s job.
