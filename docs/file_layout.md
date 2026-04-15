# File Layout

Normalized file layout so one API and one set of `Df` functions cover all data types — no per-dataset loaders to memorize.

Four patterns:

| Pattern | Path |
|---|---|
| `None` | `df_type/YYYYMMDD.parquet` |
| `hive_sym` | `df_type/sym=ABC/YYYYMMDD.parquet` |
| `single` | `df_type/part0.parquet` |
| `single_hive_sym` | `df_type/sym=ABC/part0.parquet` |


