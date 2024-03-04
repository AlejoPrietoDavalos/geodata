from abc import ABC, abstractproperty
from typing import List

from pydantic import BaseModel, Field

class GeoZoneModel(ABC, BaseModel):
    country_code: str
    country_id_csc: int
    latitude: float
    longitude: float
    postal_codes_wikidata: List[str] = Field(default_factory=list)
    websites_wikidata: List[str] = Field(default_factory=list)

    @abstractproperty
    def id_csc(self) -> int:
        """ Country/State/City id csc."""
        ...

    @abstractproperty
    def id_wikidata(self) -> str | None:
        """ Country/State/City id wikidata."""
        ...