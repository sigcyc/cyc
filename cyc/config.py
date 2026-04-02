from pathlib import Path
import yaml
from .types import DfType

_DF_TYPES_PATH = Path(__file__).resolve().parent / "files" / "df_types.yaml"


def _load_yaml() -> dict:
    with _DF_TYPES_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_data_dir() -> str:
    return _load_yaml()["data_dir"]


def get_df_type_dict(df_type: str) -> DfType:
    return _load_yaml()[df_type]


def get_calendar(df_type: str) -> str:
    config = _load_yaml()
    return config[df_type].get("calendar", config["default"].get("calendar", "nyse"))


def get_data_path(df_type: str) -> Path:
    config = _load_yaml()
    data_path = (config[df_type].get("data") or {}).get("path", config["data_dir"])
    return Path(data_path).expanduser()
