from abc import ABC, abstractproperty
from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

class GeoZoneModel(ABC, BaseModel):
    created_time: datetime
    updated_time: datetime
    country_code: str
    country_id_csc: int
    latitude: float
    longitude: float
    postal_codes_wikidata: List[str] = Field(default_factory=list)
    websites_wikidata: List[str] = Field(default_factory=list)

    @abstractproperty
    def name(self) -> str:
        """ CSC name of the entity. Sometimes English, sometimes not."""
        ...

    @abstractproperty
    def name_native(self) -> str | None:
        """ Native name of the entity."""
        ...

    @abstractproperty
    def name_english(self) -> str | None:
        """ English name of the entity."""
        ...

    @abstractproperty
    def id_csc(self) -> int:
        """ Country/State/City id csc."""
        ...

    @abstractproperty
    def id_wikidata(self) -> str | None:
        """ Country/State/City id wikidata."""
        ...