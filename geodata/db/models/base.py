from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Literal

from pydantic import BaseModel, Field

DOWN_ID_WIKIDATA = "down_id_wikidata"
DOWN_WEBSITES_POSTALS = "down_websites_postals"
DOWN_NAME_NATIVE_ENGLISH = "down_name_native_english"
DOWN_POSTALS_WIKIPEDIA = "down_postals_wikipedia"
DownType = Literal["down_id_wikidata", "down_websites_postals",
                   "down_name_native_english", "down_postals_wikipedia"]
STATUS = "status"

OK = "ok"
EXEC = "exec"
DownStatus = Literal["ok", "exec"]

class Status(BaseModel):
    down_id_wikidata: DownStatus = OK
    down_websites_postals: DownStatus = OK
    down_name_native_english: DownStatus = OK
    down_postals_wikipedia: DownStatus = OK


class GeoZoneModel(ABC, BaseModel):
    created_time: datetime
    updated_time: datetime
    country_code: str
    country_id_csc: int
    latitude: float
    longitude: float
    websites_wikidata: List[str] = Field(default_factory=list)
    postal_codes_wikidata: List[str] = Field(default_factory=list)
    status: Status = Field(default_factory=Status)

    @property
    @abstractmethod
    def name(self) -> str:
        """ CSC name of the entity. Sometimes English, sometimes not."""
        ...

    @property
    @abstractmethod
    def name_native(self) -> str | None:
        """ Native name of the entity."""
        ...

    @property
    @abstractmethod
    def name_english(self) -> str | None:
        """ English name of the entity."""
        ...

    @property
    @abstractmethod
    def id_csc(self) -> int:
        """ Country/State/City id csc."""
        ...

    @property
    @abstractmethod
    def id_wikidata(self) -> str | None:
        """ Country/State/City id wikidata."""
        ...