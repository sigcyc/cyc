from __future__ import annotations
from typing import Optional, Sequence, overload
import polars as pl


class MergeHelper:
    _idx: pl.Series
    _right_len: int

    def __init__(self, idx, right_len: int) -> None:
        self._idx = idx
        self._right_len = right_len
    @classmethod
    def merge(
        cls,
        left_on: pl.Series,
        right_on: pl.Series,
        by_left: Optional[pl.Series | Sequence[pl.Series]] = None,
        by_right: Optional[pl.Series | Sequence[pl.Series]] = None,
    ) -> "MergeHelper":
        """
        Perform join_asof and return indices for gathering from right series.
        """
        if isinstance(by_left, pl.Series):
            by_left = [by_left]
        if isinstance(by_right, pl.Series):
            by_right = [by_right]
        by_cols = [f"_by_{i}" for i in range(len(by_left or []))]

        left_df = pl.DataFrame({
            "_key": left_on,
            **{c: s for c, s in zip(by_cols, by_left or [])},
        })
        right_df = pl.DataFrame({
            "_key": right_on,
            "_idx": pl.arange(0, len(right_on), eager=True),
            **{c: s for c, s in zip(by_cols, by_right or [])},
        })
        return cls(left_df.join_asof(right_df, on="_key", by=by_cols or None)["_idx"], len(right_on))

    @overload
    def get(self, item: pl.Series) -> pl.Series: ...
    @overload
    def get(self, item: pl.DataFrame) -> pl.DataFrame: ...
    def get(self, item: pl.Series | pl.DataFrame) -> pl.Series | pl.DataFrame:
        item_len = len(item)
        if item_len != self._right_len:
            raise ValueError(f"item length {item_len} != right_on length {self._right_len}")
        if isinstance(item, pl.Series):
            return item.gather(self._idx)
        return item.select(pl.all().gather(self._idx))



