from typing import Optional

from geodata.db.models.base import GeoZoneModel

class City(GeoZoneModel):
    city_id_csc: int
    state_id_csc: int
    city_name: str
    state_code: Optional[str]
    city_id_wikidata: Optional[str] = None