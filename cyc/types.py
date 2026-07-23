from typing import Iterable, Literal, NotRequired, Optional, TypedDict

SymType = Optional[str | int | Iterable[str | int]]


class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: NotRequired[str]
    date: NotRequired[str]
    enum: NotRequired[dict[str, str]]  # column name -> cyc_types enum class name
    enrich: NotRequired[str]  # module in cyc.enrich exposing enrich(lf) -> lf
    data: dict[str, str]
    calendar: str # time zone and trading-day calendar
    file_layout: NotRequired[Optional[Literal["hive_sym", "single", "single_hive_sym"]]]
