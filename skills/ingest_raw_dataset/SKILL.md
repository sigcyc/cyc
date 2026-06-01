---
name: ingest_raw_dataset
description: Ingest a raw dataset into the project's normalized parquet storage layout
argument-hint: DATASET path file_layout
---

## Inputs

- `DATASET` — the raw dataset: a directory or a single file. Also the working directory for this skill's own artifacts (`columns.yaml` when any column needs renaming, `convert.py`).
- `path` — where to write the normalized output. Registered as `data.path` and resolved by `cyc.config.get_data_path("DATASET")`.
- `file_layout` — target layout, one of the patterns in `~/.claude/skills/ingest_raw_dataset/data_pipeline.md`.

## Reference — use these; do NOT open `cyc/` source to re-derive them

The only `cyc` facts this skill needs are below. Reading `config.py`, `data_loaders.py`, or `df.py` to rediscover them is wasted work — they do not change.

- **`cyc.config.get_data_path(DATASET) -> Path`** — output root. Equals `<data.path>/DATASET`.
- **Verify** (step 6): `Df.load_data(DATASET, date) -> Df`; then `df.head(10)._A` (side-effecting pretty-printer, prints to stdout).
- **File layouts → on-disk paths**: the four patterns (`date`, `hive_sym`, `single`, `single_hive_sym`) are tabulated in `data_pipeline.md`, symlinked in this skill dir. Read that file, not the loader source.
- **`df_types.yaml` entry shape** (step 4):
  ```yaml
  DATASET:
    cols:
      core: [sym, time]   # or [sym, date]
    sym: sym              # canonical name produced by convert.py (post-rename)
    time: time            # or  date: date
    file_layout: date     # default 'date' may be omitted; else hive_sym / single / single_hive_sym
    data:
      path: <path input>  # so get_data_path(DATASET) == <path>/DATASET
  ```
- **Conversion script** (step 5): copy `convert_template.py` (in this dir) to `DATASET/convert.py` and fill the `<...>` placeholders. Do not author it from scratch.

## Steps

1. **Survey the filesystem layout**
   - Walk `DATASET` and summarize its directory structure only (e.g. `DATASET/SYMBOL.csv`, `DATASET/SYMBOL/YYYY-MM-DD.csv`).
   - Do NOT open or read any file yet.

2. **Preserve raw data**
   - If `DATASET` and `path` resolve to the same location, conversion would overwrite the raw data. In that case, rename `DATASET` → `DATASET_raw` first.
   - From here on, read raw data from `raw_src`: `DATASET_raw` if you renamed in this step, otherwise `DATASET`.
   - Never modify `raw_src` after this point.

3. **Survey file contents and build the column mapping (only if needed)**
   - Read ONE representative file from `raw_src`.
   - Determine which columns need renaming:
     - Non-English columns: translate to English.
     - English columns containing ` ` or `-`: replace ` ` and `-` with `_`.
   - **If at least one column needs renaming:** create `DATASET/columns.yaml` mapping `raw_name: output_name` for those columns only, then ask the user to review it before proceeding and monitor the file for edits.
   - **If no column needs renaming:** skip this step entirely — do NOT create `columns.yaml`, and do not ask the user to review anything. The convert script passes every column through unchanged. (Required output columns like `sym`/`date` that don't exist in the raw file are derived in `convert.py`, not via a rename, so they never belong in `columns.yaml`.)

4. **Register in `cyc/files/df_types.yaml`**
   - `cyc` is the util package installed in the Python environment.
   - Add an entry for `DATASET` with: `sym`, `time` (or `date`), `data.path`, `file_layout` (entry shape in Reference above).
   - Set `data.path` to the `path` input, so `cyc.config.get_data_path("DATASET")` resolves the output directory.
   - If `DATASET` is a single file, set `file_layout` to `single`.
   - Do this before writing the script so the script can call `get_data_path("DATASET")`.

5. **Write a conversion script at `DATASET/convert.py`**
   - Start from `convert_template.py` in this skill dir (see Reference); copy it and fill the `<...>`. Do not author from scratch or open `cyc/` source.
   - Put the script in the dataset directory (not inline / not in a notebook) so it is reproducible and version-controllable.
   - Responsibilities:
     - Read one raw file from `raw_src`.
     - Rename the columns listed in `DATASET/columns.yaml` if that file exists; if it does not exist, perform no renames.
     - Pass through all other columns with names unchanged.
     - Cast to the required output schema (see below).
     - Write parquet files in the `file_layout` layout, rooted at `cyc.config.get_data_path("DATASET")`.
   - **Output schema requirement**: every output file MUST contain:
     - `sym` — `pl.String` or `pl.UInt64`
     - At least one of (both allowed):
       - `time` — `pl.Datetime("ns")`
       - `date` — `pl.Date`
   - **Do NOT drop any columns** from the raw file, even if they look redundant.

6. **Dry-run on a single file and verify**
   - Run `DATASET/convert.py` on ONE raw file.
   - Verify via the project loader (do not just inspect the parquet directly). You MUST run this exact snippet via `python -c` (or equivalent) and paste the real stdout into chat — do NOT substitute `print(df)`, `df.df`, or any other rendering, and do NOT paraphrase the output. `_A` is a side-effecting pretty-printer; its output is the artifact the user reviews:
     ```python
     from cyc import Df
     df = Df.load_data("DATASET", <date>)
     df.head(10)._A   # sanity check — prints to stdout
     ```
   - Show the raw stdout to the user and WAIT for explicit confirmation before proceeding.

7. **Convert all data**
   - Only after step 6 is confirmed, run `DATASET/convert.py` across the full date range.

## Safety Rules

- NEVER read, write, move, or delete any files outside `raw_src` (`DATASET` / `DATASET_raw`), the `DATASET` working directory, and the output `path`.
- Do NOT list, touch, or inspect any other parent or sibling folders.
- `cyc/files/df_types.yaml` is the only file you may edit outside those locations (step 4).
