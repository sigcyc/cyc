# Change: Rework Df load_data

I want cyc/df.py:_enrich to have the following logic
1. add time, date based on df_types.yaml if doesn't exist
2. if both time, date exist - return
3. if only time exist, compute date from time
4. if only date exist, comptue time from date
