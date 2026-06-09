# Change: plot enhancement


The basic idea of the current plotting under cyc/data_frame_monkey_patch.py:_plot, is that we first prepare the dataframe, and plot the columns on the left_axis and right_axis. There are multiple cases where we want to plot the same column but with different filters. 

The first case is that the column to plot is spot, and we want to plot spot for different symbol. And the second case is that we want to plot 13f data, we want to plot the position for different hedge fund. 

I'm thinking to change _plot: left/right axis to list[int | str | tuple[int | str, pl.Expr]]. When it's tuple[int | str, pl.Expr], it will plot the filtered columns. It will have name "{column_name}_{pl.Expr_name}".

Add as little code as possible. For example add PlotSpec together (I haven't thought through if that works).
