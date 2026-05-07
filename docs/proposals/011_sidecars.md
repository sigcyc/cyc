# Change: sidecars support

Status: done. See `docs/data_pipeline.md` for the final shape.

## What

A sidecar is a df_type of the form `df_type__sidecar_name`:
1. No entry needed in `df_types.yaml` — `calendar` and `file_layout` are inherited from the parent
2. Data lives at `<parent_data_path>/df_type/sidecar_name/...` following the parent's layout pattern

## What changed

1. `cyc/config.py`: `get_calendar`, `get_file_layout`, `get_data_path` resolve to the parent for sidecar df_types via `_parent`. `get_data_path` returns the data root including the sidecar suffix (`__` becomes `/`). `get_file_layout` defaults to `"date"` when omitted.
2. `cyc/data_loaders.py`: `load_data(df_type, date_str=None, sym=None) -> pl.LazyFrame` is the canonical dispatcher; the by-date loader is `load_data_date`. Unknown layouts raise `ValueError`.
3. `Df.load_sidecars(sidecars: list[str]) -> None`: derives `date_list` and `sym` from `self.df`, loads each sidecar parquet via the dispatcher, and merges columns into `self.df` via `with_columns`. Same-named columns are overwritten; mismatched row counts raise `ShapeError`.
