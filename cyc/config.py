from pathlib import Path
import yaml
from .types import DfType

_DF_TYPES_PATH = Path(__file__).resolve().parent / "files" / "df_types.yaml"


def _load_yaml() -> dict:
    with _DF_TYPES_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_df_type_parent(df_type: str) -> str:
    return df_type.split("__", 1)[0]

def get_df_type_dict(df_type: str) -> DfType:
    return _load_yaml()[df_type]


def get_calendar(df_type: str) -> str:
    config = _load_yaml()
    return config[get_df_type_parent(df_type)].get("calendar", config["default"]["calendar"])


def get_file_layout(df_type: str) -> str | None:
    config = _load_yaml()
    return config[get_df_type_parent(df_type)].get("file_layout", "date")


def get_data_path(df_type: str) -> Path:
    """Directory containing the parquet files (or partition dirs) for df_type.

    For a sidecar `parent__name`, returns `<data_path>/parent/name`.
    """
    config = _load_yaml()
    parent = get_df_type_parent(df_type)
    base = (config[parent].get("data") or {}).get("path", config["data_dir"])
    return Path(base).expanduser() / df_type.replace("__", "/")
