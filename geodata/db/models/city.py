from typing import Optional

from geodata.db.models.base import GeoZoneModel

class City(GeoZoneModel):
    city_id_csc: int
    state_id_csc: int
    city_name: str
    state_code: Optional[str]
    city_id_wikidata: Optional[str] = None
    
    @property
    def id_csc(self) -> int:
        return self.city_id_csc

    @property
    def id_wikidata(self) -> str | None:
        """ `city_id_wikidata`."""
        return self.city_id_wikidata