from typing import TypedDict


class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]
    calendar: str
