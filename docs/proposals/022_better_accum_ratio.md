# Proposal: better accum ratio proposal

Right now, cyc/data_analysis implement the accum_ratio that's very helpful to me. However, I need at least two more functions (sum, len) that needs the same preprocessing of group_by, pivot, col_sum. So I think it's worth extrapolating the stuffs out. I'm thinking the new signature would be

`df.accum_ratio('days_to_exp_cut', ['index', 'quarter_cat'], "spread", 1)` to `df.gb2('days_to_exp_cut', ['index', 'quarter_cat']).ratio("spread", 1)`. Here are a couple things that I'm thinking
1. Is `_resolve_columns` needed
2. Looks like `AccumRatioResult` can be used for `GroupByResult`
3. Determine if gb2 or gb. This is a function that will be called a lot so gb and gb2 won't be confusing in general.
4. The code should be elegant and beautiful. Do NOT rebuild the wheels
5. Do NOT worry about any existing tests. Feel free to break and delete
