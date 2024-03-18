from typing import Optional

from geodata.db.models.base import GeoZoneModel

class Country(GeoZoneModel):
    region_id_csc: int
    country_name: str
    country_name_native: Optional[str] = None
    phone_code: str
    country_id_wikidata: Optional[str] = None
    
    @property
    def name(self) -> str:
        return self.country_name
    
    @property
    def name_native(self) -> str | None:
        return self.country_name_native
    
    @property
    def id_csc(self) -> int:
        return self.country_id_csc

    @property
    def id_wikidata(self) -> str | None:
        """ `country_id_wikidata`."""
        return self.country_id_wikidata