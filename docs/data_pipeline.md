# Data Pipeline

This doc describes how files are layout in the filesystem, and how the data_loaders are designed to load the data


## File Layout
Normalized file layout so one API and one set of `Df` functions cover all data types — no per-dataset loaders to memorize.

Four patterns:

| Pattern           | Path                               |
|-------------------|------------------------------------|
| `None`            | `df_type/YYYYMMDD.parquet`         |
| `hive_sym`        | `df_type/sym=ABC/YYYYMMDD.parquet` |
| `single`          | `df_type/part0.parquet`            |
| `single_hive_sym` | `df_type/sym=ABC/part0.parquet`    |


## Data Loaders

`Df.load_data(df_type, date_str, sym=None)` dispatches on `file_layout` (from `df_types.yaml`) to one of three loaders in `cyc/data_loaders.py`. Non-hive paths wrap the result with `filter_sym(lf, sym)` to restrict rows when `sym` is given.

| `file_layout` | Loader | Behavior |
|---|---|---|
| `None` | `load_data(df_type, date_str)` | Resolves each date to `df_type/YYYYMMDD.parquet`, skips missing files (prints `missing_dates`), and `scan_parquet`s the surviving list. `sym` filter applied via `filter_sym`. |
| `hive_sym` | `load_data_hive_sym(df_type, date_str, sym)` | Enumerates `sym=*` subdirs when `sym is None`, else uses the passed syms. Reads `df_type/sym=S/YYYYMMDD.parquet` per (sym, date) with `hive_partitioning=True` so `sym` is restored as a column. |
| `single` | `load_data_single(df_type)` | Scans `df_type/` as a single hive-partitioned dataset; `date_str` is ignored. |
| `single_hive_sym` | `load_data_single(df_type)` | Same as `single` — `hive_partitioning=True` picks up the `sym=...` partition automatically. |

Date resolution: `date_str` may be a `pl.Series` of dates, or a string parsed via `parse_dates` against the df_type's `calendar` (supports ranges like `20250101-20250131`). Output is a `pl.LazyFrame`; `Df._enrich` then aliases the configured `sym`/`time` columns to the canonical names before collecting.

