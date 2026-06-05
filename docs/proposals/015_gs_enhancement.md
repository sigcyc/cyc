# Change: gs enhancement

I want to modify cyc/gui.py gs function to support group_by stacking. What I want to happen is that

Add a parameter group_by: IntoExpr | Iterable[IntoExpr] similar as pl.DataFrame.group_by.

Given a group_by, gs will create the lines and scatter plots for each of group_by.

For example, say group_by='fruit'. df fruit column has three unique values "apple", "orange", "cherry". Then it will create one linear regression line with legend apple_0.23x+0.4_r2=0.2_n=52

Try to use exciting functions as much as possible. e.g., df1.gs() + df2.gs() for example
