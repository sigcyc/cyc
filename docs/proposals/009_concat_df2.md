# Change: concat_df2

I want to add a function
`def concat_df2(df1: pl.DataFrame | Df, df2: pl.DataFrame | Df) -> pl.DataFrame` in cyc/df_util.py.

## Goal
The function will combine the columns of two dataframe together. The important part is the naming of columns and order.

1. Say the first df have columns df1_col1, df1_col2, the second df has columns df2_col1, df2_col2. The output column will have the order
df1_col1_x, df2_col1_x, df1_col2_x, df2_ocl2_x. Note that we add suffix _x to df1, _y to df2.

2. When one df has more columns than another way say, df1 has col1, col2, col3, col4, df2 has col1, col2, the output will be

col1_x, col1_y, col2_x, col2_y, col3_x, col4_x

## Requirement
Do not reinvent the wheels. Use existing polars function if exists even
