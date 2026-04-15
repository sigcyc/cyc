from typing import Iterable, Literal, NotRequired, Optional, TypedDict

SymType = Optional[str | int | Iterable[str | int]]


class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]
    calendar: str
    file_layout: NotRequired[Optional[Literal["hive_sym", "single", "single_hive_sym"]]]
