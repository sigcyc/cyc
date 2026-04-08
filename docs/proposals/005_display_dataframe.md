# Goal: Display full DataFrame

# Context:
Currently @cyc/data_frame_monkey_patch.py:_print_all function print the full dataframe. It will print the first chunk_size and then the next chunk_size and so on. The problem is that if there is a field like string that's very long, there will be multiple lines for one row, which is not what I want. 

What I want is that I estimate the width of each column. Include column until it reaches tbl_width_chars and return


# Plan:

1. Look at the polars rust implementation and find how per-column width is calculated.
2. Reproduce the logic in python
3. Fine to skip any logic when tbl_cols is not -1.
4. Change the logic _print_all to add estimated size until it reaches tbl_width_chars and then return

