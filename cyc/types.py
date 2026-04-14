from typing import Iterable, Optional, TypedDict

SymType = Optional[str | int | Iterable[str | int]]

class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]
    calendar: str
