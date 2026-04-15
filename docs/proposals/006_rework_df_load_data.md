# Change: Rework Df load_data

Status: done. See `docs/data_pipeline.md` for the final shape.

## What changed

1. Swapped loader arg order to `(df_type, date_str[, sym])` across `load_data`, `load_data_hive_sym`, and `_get_date_list` in `cyc/data_loaders.py`.
2. `Df.load_data(df_type, date_str=None, sym=None)` now dispatches on `file_layout` via `match`/`case`:
   - `hive_sym` → `load_data_hive_sym`
   - `single` / `single_hive_sym` → `load_data_single` (ignores `date_str`)
   - default → `load_data`
3. Removed `Df.load_data_single` classmethod — single layouts go through `Df.load_data`.
4. Added `filter_sym(lf, sym)` util in `cyc/df.py`, applied after `_enrich`.
5. `date_str` is `None` for `single` / `single_hive_sym`; required for the other two branches (enforced via `assert`).
