# Change: Refactor data_loaders

## What

Refactor data_loaders to make it more extensible

## Detail
1. in data_loaders.py, add a function load_data_hive_sym(date_str, df_type, sym) that load hive data. Do not use glob. Use scan_parquet
2. in data_loaders.py, refactor function load_data(date_str, df_type) that only works data_root/YYYYMMDD.parquet.
3. add a storage_pattern in df_types.yaml. If None, default to data_root/YYYYMMDD.parquet. If hive_sym, the path is data_root/sym=AAPL/YYYYMMDD.parquet
4. in Df.load_data, route to data_loaders/load_data, data_loaders/load_data_hive_sym based on storage_pattern
