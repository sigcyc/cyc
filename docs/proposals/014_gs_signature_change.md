# Change: gs signature change

I want to change cyc/gui.py:gs to 

def gs(df: pl.DataFrame, x: pl.Expr, y: pl.Expr, k: int = 10, f: pl.Expr = pl.lit(True)

I also want to add data_frame_monkey_patch
