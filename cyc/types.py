from pathlib import Path
from typing import TypedDict
import yaml
class DfType(TypedDict):
    cols: dict[str, list[str]]
    sym: str
    time: str
    data: dict[str, str]


def get_df_type_dict(df_type: str) -> DfType:
    """
    Load yaml file from cyc/files/df_types.yaml. Return the entry with df_type
    """
    df_types_path = Path(__file__).resolve().parent / "files" / "df_types.yaml"
    with df_types_path.open("r", encoding="utf-8") as file:
        df_types = yaml.safe_load(file) or {}
    return df_types[df_type]

