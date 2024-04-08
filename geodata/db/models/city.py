from typing import Optional, List

from pydantic import Field

from geodata.db.models.base import GeoZoneModel

class City(GeoZoneModel):
    city_id_csc: int
    state_id_csc: int
    city_name: str
    city_name_native: Optional[str] = None
    city_name_english: Optional[str] = None
    state_code: Optional[str] = None
    city_id_wikidata: Optional[str] = None
    postal_codes_wikipedia: List[str] = Field(default_factory=list)
    postal_codes_wikipedia_clean: List[str] = Field(default_factory=list)
    
    @property
    def name(self) -> str:
        return self.city_name
    
    @property
    def name_native(self) -> str | None:
        return self.city_name_native

    @property
    def name_english(self) -> str | None:
        """ English name of the entity."""
        return self.city_name_english
    
    @property
    def id_csc(self) -> int:
        return self.city_id_csc

    @property
    def id_wikidata(self) -> str | None:
        """ `city_id_wikidata`."""
        return self.city_id_wikidata