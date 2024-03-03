from typing import List

from pydantic import BaseModel, Field

class GeoZoneModel(BaseModel):
    country_code: str
    country_id_csc: int
    latitude: float
    longitude: float
    postal_codes_wikidata: List[str] = Field(default_factory=list)
    websites_wikidata: List[str] = Field(default_factory=list)