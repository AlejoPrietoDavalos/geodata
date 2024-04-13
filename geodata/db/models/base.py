from abc import ABC, abstractmethod
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
    websites_wikidata: List[str] = Field(default_factory=list)
    postal_codes_wikidata: List[str] = Field(default_factory=list)

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