# Plan


My use case is that I want to add a ds_type to a polars dataframe object. As a result, I can do operations on dataframe based on ds_type. The main problem is that after I run function like df.select, I want to keep this ds_type. Everyone is trying to convince me not to inherit pl.DataFrame, and I'm ok with that. But my LSP no longer pop the pl.DataFrame functions, make sure deal with that. A reference verision is under cyc/ds.py. Implement the code in ds2.py
