from pathlib import Path
from typing import TypedDict
import yaml


class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]


_DF_TYPES_PATH = Path(__file__).resolve().parent / "files" / "df_types.yaml"


def _load_yaml() -> dict:
    with _DF_TYPES_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_df_type_dict(df_type: str) -> DfType:
    return _load_yaml()[df_type]


def get_data_path(df_type: str) -> Path:
    config = _load_yaml()
    data_path = (config[df_type].get("data") or {}).get("path", config["data_dir"])
    return Path(data_path).expanduser()
