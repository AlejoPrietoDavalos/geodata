from typing import Type, TypeVar
from pathlib import Path
import json

from pydantic import BaseModel

__all__ = ["Cfg"]

T_Cfg = TypeVar("T_Cfg", bound="Cfg")

class Cfg(BaseModel):
    db_name: str = "world_data"

    @classmethod
    def path_cfg(cls) -> Path:
        return Path("config.json")

    @classmethod
    def from_json(cls: Type[T_Cfg]) -> T_Cfg:
        path_cfg = cls.path_cfg()

        if path_cfg.exists():
            with open(path_cfg, "r") as f:
                cfg = cls(**json.load(f))       # If it exists, read it.
        else:
            with open(path_cfg, "w") as f:
                cfg = cls()                     # If the config.json is not there, create it.
                json.dump(cfg.model_dump(), f)
        return cfg
    