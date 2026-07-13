# Data Pipeline

This doc describes how files are layout in the filesystem, and how the data_loaders are designed to load the data


## File Layout
Normalized file layout so one API and one set of `Df` functions cover all data types — no per-dataset loaders to memorize.

Four patterns:

| Pattern           | Path                               |
|-------------------|------------------------------------|
| `date` (default)  | `df_type/YYYYMMDD.parquet`         |
| `hive_sym`        | `df_type/sym=ABC/YYYYMMDD.parquet` |
| `single`          | `df_type/part0.parquet`            |
| `single_hive_sym` | `df_type/sym=ABC/part0.parquet`    |

### Sidecars

`df_type__sidecar_name` is a sidecar of `df_type`. It needs no entry in `df_types.yaml` — it inherits `calendar` and `file_layout` from the parent and lives at `df_type/sidecar_name/...` following the parent's layout pattern.


## Data Loaders

`cyc.data_loaders.load_data(df_type, date_str=None, sym=None) -> pl.LazyFrame` dispatches on `file_layout` (from `df_types.yaml`, defaulting to `date`) to one of the layout-specific loaders. Unknown layouts raise `ValueError`.

| `file_layout` | Loader | Behavior |
|---|---|---|
| `date` | `load_data_date(df_type, date_str)` | Resolves each date to `df_type/YYYYMMDD.parquet`, or to `df_type/date=YYYYMMDD/*.parquet` when the flat file is absent (both forms may coexist in one dataset). Skips dates with no files (prints `missing_dates`) and concats one scan per date, stamping the `date` column from the label. |
| `hive_sym` | `load_data_hive_sym(df_type, date_str, sym)` | Enumerates `sym=*` subdirs when `sym is None`, else uses the passed syms. Reads `df_type/sym=S/YYYYMMDD.parquet` per (sym, date) with `hive_partitioning=True` so `sym` is restored as a column. |
| `single` | `load_data_single(df_type)` | Scans `df_type/` as a single hive-partitioned dataset; `date_str` is ignored. |
| `single_hive_sym` | `load_data_single(df_type)` | Same as `single` — `hive_partitioning=True` picks up the `sym=...` partition automatically. |

Date resolution: `date_str` may be a `pl.Series` of dates, or a string parsed via `parse_dates` against the df_type's `calendar` (supports ranges like `20250101-20250131`).

`Df.load_data(df_type, date_str=None, sym=None) -> Df` wraps the dispatcher: applies `Df._enrich` (aliases the configured `sym`/`time` columns to the canonical names), filters rows by `sym` via `filter_sym`, then collects.

`Df.load_sidecars(sidecars: list[str]) -> None` loads each sidecar parquet using the parent Df's date and sym range and merges the columns into `self.df` via `with_columns` — same-named columns are overwritten; mismatched row counts raise `ShapeError`.
