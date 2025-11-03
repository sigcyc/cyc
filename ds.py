from typing import Optional
import polars as pl
import shutil

pl.Config.set_tbl_formatting("UTF8_FULL")


class Ds(pl.DataFrame):
    @property
    def _T(self):
        """Return the full column representation without altering global config."""
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_formatting="ASCII_FULL_CONDENSED"):
            print(repr(self.head(3).transpose(include_header=True)))

    @property
    def _A(self, float_precision: Optional[int] = 4):
        terminal_width = shutil.get_terminal_size().columns
        chunk_size = terminal_width // 12
        with pl.Config(
            tbl_rows=-1,
            tbl_cols=-1,
            tbl_width_chars=terminal_width,
            float_precision=float_precision,
        ):
            for start in range(0, len(self.columns), chunk_size):
                cols = self.columns[start : start + chunk_size]
                print(repr(self.select(cols)))
                if start + chunk_size < len(self.columns):
                    print()

    def __getattr__(self, name):
        if name in self.columns:
            return self[name]
        return super().__getattr__(name)
