# Change: Refactor data_loaders

## What

Refactor data_loaders to make it more extensible

## Detail
1. data_root is always df_type
2. There are the cases how the data is organized. In the first case, each file will be named data_root/20250101.parquet
3. In the second case, it will have a hive_partitioning like data_root/sym=AAPL/20250101.parquet. I might add more depth in the future. Prioritize clean code at the moment
4. It will have some a progress bar
5. It will print the dates where no data is found
6. Rely on existing packages and write minimal code



