---
name: ingest_raw_dataset
description: Ingest a raw dataset into the project's normalized parquet storage layout
argument-hint: file_layout
---

## Inputs

- `ABC` — the raw dataset directory (working directory for this skill)
- `file_layout` — target layout, one of the patterns in `~/.claude/skills/ingest_raw_dataset/data_pipeline.md`

## Steps

1. **Preserve raw data**
   - Rename `ABC` → `ABC_raw`. Never modify `ABC_raw` after this point.

2. **Survey the filesystem layout**
   - Walk `ABC_raw` and summarize its directory structure only (e.g. `ABC_raw/SYMBOL.csv`, `ABC_raw/SYMBOL/YYYY-MM-DD.csv`).
   - Do NOT open or read any file yet.

3. **Survey file contents and build column mapping**
   - Read ONE representative file from `ABC_raw`.
   - Create `ABC/columns.yaml` mapping `raw_name: output_name`.
   - **Which columns to include in `columns.yaml`:**
     - Non-English columns: include (translate to English).
     - English columns that has ` ` or `-`: include (replace ` ` and `-` by `_`).
   - Ask the user to review `ABC/columns.yaml` before proceeding. Monitor the file for edits.

4. **Register in `cyc/files/df_types.yaml`**
   - `cyc` is the util package installed in the Python environment.
   - Add an entry for `ABC` with: `sym`, `time` (or `date`), `data.path`, `file_layout`.
   - Do this before writing the script so the script can use `cyc.config.get_data_path("ABC")` to resolve the output directory.

5. **Write a conversion script at `ABC/convert.py`**
   - Put the script in the dataset directory (not inline / not in a notebook) so it is reproducible and version-controllable.
   - Responsibilities:
     - Read one raw file from `ABC_raw`.
     - Rename all columns in `ABC/columns.yaml`.
     - Pass through all the other columns with names unchanged.
     - Cast to the required output schema (see below).
     - Write parquet files into the `file_layout` layout rooted at `cyc.config.get_data_path("ABC")`.
   - **Output schema requirement**: every output file MUST contain:
     - `sym` — `pl.String` or `pl.UInt64`
     - At least one of (both allowed):
       - `time` — `pl.Datetime("ns")`
       - `date` — `pl.Date`
   - **Do NOT drop any columns** from the raw file, even if they look redundant. 

6. **Dry-run on a single file and verify**
   - Run `ABC/convert.py` on ONE raw file.
   - Verify via the project loader (do not just inspect the parquet directly). You MUST run this exact snippet via `python -c` (or equivalent) and paste the real stdout into chat — do NOT substitute `print(df)`, `df.df`, or any other rendering, and do NOT paraphrase the output. `_A` is a side-effecting pretty-printer; its output is the artifact the user reviews:
     ```python
     from cyc import Df
     df = Df.load_data("ABC", <date>)
     df.head(10)._A   # sanity check — prints to stdout
     ```
   - Show the raw stdout to the user and WAIT for explicit confirmation before proceeding.

7. **Convert all data**
   - Only after step 6 is confirmed, run `ABC/convert.py` across the full date range.

## Safety Rules

- NEVER read, write, move, or delete any files outside `ABC_raw` and `ABC`.
- Do NOT list, touch, or inspect the parent directory or any sibling folders.
- The only file allowed outside those two directories is `cyc/files/df_types.yaml` (step 4).
