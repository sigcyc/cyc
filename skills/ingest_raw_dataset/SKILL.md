---
name: ingest_raw_dataset
description: Ingest a raw dataset into the project's normalized parquet storage layout
argument-hint: storage_pattern
---

## Steps

Given a raw dataset `ABC` and a target storage pattern (passed as argument, see `storage_pattern.md`):

1. **Preserve raw data** — Rename `ABC` to `ABC_raw` so the original is never overwritten.

2. **Survey the raw layout** — Walk through `ABC_raw` and summarize its structure (e.g. `ABC_raw/SYMBOL.csv`, `ABC_raw/SYMBOL/YYYY-MM-DD.csv`).

3. **Register in `df_types.yaml`** — Add an entry for `ABC` with the correct `cols`, `sym`, `time`, `data.path`, `storage_pattern`, and `calendar`.

4. **Write a conversion script** — Convert raw files to parquet in the target layout. Translate any non-English column names to English. Run on a single date first and ask the user to confirm the output looks correct.

6. **Convert all data** — Run the full conversion.

7. **Save the ingest recipe** — Record the commands used in `ABC/ingest.md` for reproducibility.
