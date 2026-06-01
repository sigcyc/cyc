---
name: ingest_raw_dataset
description: Ingest a raw dataset into the project's normalized parquet storage layout
argument-hint: DATASET path file_layout
---

## Inputs

- `DATASET` — the raw dataset: a directory or a single file. Also the working directory for this skill's own artifacts (`columns.yaml`, `convert.py`).
- `path` — where to write the normalized output. Registered as `data.path` and resolved by `cyc.config.get_data_path("DATASET")`.
- `file_layout` — target layout, one of the patterns in `~/.claude/skills/ingest_raw_dataset/data_pipeline.md`.

## Steps

1. **Survey the filesystem layout**
   - Walk `DATASET` and summarize its directory structure only (e.g. `DATASET/SYMBOL.csv`, `DATASET/SYMBOL/YYYY-MM-DD.csv`).
   - Do NOT open or read any file yet.

2. **Preserve raw data**
   - If `DATASET` and `path` resolve to the same location, conversion would overwrite the raw data. In that case, rename `DATASET` → `DATASET_raw` first.
   - From here on, read raw data from `raw_src`: `DATASET_raw` if you renamed in this step, otherwise `DATASET`.
   - Never modify `raw_src` after this point.

3. **Survey file contents and build column mapping**
   - Read ONE representative file from `raw_src`.
   - Create `DATASET/columns.yaml` mapping `raw_name: output_name`.
   - **Which columns to include in `columns.yaml`:**
     - Non-English columns: include (translate to English).
     - English columns containing ` ` or `-`: include (replace ` ` and `-` with `_`).
   - Ask the user to review `DATASET/columns.yaml` before proceeding. Monitor the file for edits.

4. **Register in `cyc/files/df_types.yaml`**
   - `cyc` is the util package installed in the Python environment.
   - Add an entry for `DATASET` with: `sym`, `time` (or `date`), `data.path`, `file_layout`.
   - Set `data.path` to the `path` input, so `cyc.config.get_data_path("DATASET")` resolves the output directory.
   - If `DATASET` is a single file, set `file_layout` to `single`.
   - Do this before writing the script so the script can call `get_data_path("DATASET")`.

5. **Write a conversion script at `DATASET/convert.py`**
   - Put the script in the dataset directory (not inline / not in a notebook) so it is reproducible and version-controllable.
   - Responsibilities:
     - Read one raw file from `raw_src`.
     - Rename all columns listed in `DATASET/columns.yaml`.
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
