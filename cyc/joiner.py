from __future__ import annotations
from typing import Literal, Optional, Sequence, TYPE_CHECKING, overload
import polars as pl

if TYPE_CHECKING:
    from .df import Df


class Joiner:
    _idx: pl.Series
    _right_len: int

    def __init__(self, idx, right_len: int) -> None:
        self._idx = idx
        self._right_len = right_len

    @classmethod
    def join_asof(
        cls,
        left_on: pl.Series,
        right_on: pl.Series,
        by_left: Optional[pl.Series | Sequence[pl.Series]] = None,
        by_right: Optional[pl.Series | Sequence[pl.Series]] = None,
    ) -> "Joiner":
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

    @classmethod
    def join(
        cls,
        left_on: pl.Series | Sequence[pl.Series],
        right_on: pl.Series | Sequence[pl.Series],
        how: Literal["first", "last"] = "last",
    ) -> "Joiner":
        """
        Left join on key(s), keeping the first/last right match per key.
        Output always has len(left_on) rows; unmatched rows produce null indices.
        """
        if isinstance(left_on, pl.Series):
            left_on = [left_on]
        if isinstance(right_on, pl.Series):
            right_on = [right_on]
        key_cols = [f"_key_{i}" for i in range(len(left_on))]
        right_len = len(right_on[0])
        left_df = pl.DataFrame({c: s for c, s in zip(key_cols, left_on)})
        right_df = pl.DataFrame({
            **{c: s for c, s in zip(key_cols, right_on)},
            "_idx": pl.arange(0, right_len, eager=True),
        }).unique(subset=key_cols, keep=how, maintain_order=True)
        return cls(left_df.join(right_df, on=key_cols, how="left", maintain_order="left")["_idx"], right_len)

    @overload
    def get(self, item: Df) -> Df: ...
    @overload
    def get(self, item: pl.Series) -> pl.Series: ...
    @overload
    def get(self, item: pl.DataFrame) -> pl.DataFrame: ...
    def get(self, item):
        if hasattr(item, "df_type"): # Df handling 
            return type(item)(self.get(item.df), item.df_type)
        item_len = len(item)
        if item_len != self._right_len:
            raise ValueError(f"item length {item_len} != right_on length {self._right_len}")
        if isinstance(item, pl.Series):
            return item.gather(self._idx)
        return item.select(pl.all().gather(self._idx))



