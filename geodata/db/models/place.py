from typing import Optional

from geodata.db.models.base import GeoZoneModel

class Place(GeoZoneModel):
    place_id_csc: int
    country_id_csc: int
    state_id_csc: int
    place_name: str
    state_code: Optional[str]
    country_code: str
    place_id_wikidata: Optional[str] = None