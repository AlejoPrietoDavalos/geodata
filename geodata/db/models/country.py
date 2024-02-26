from typing import Optional

from geodata.db.models.base import GeoZoneModel

class Country(GeoZoneModel):
    country_id_csc: int
    region_id_csc: int
    country_code: str
    country_name: str
    country_name_native: Optional[str] = None
    phone_code: str
    country_id_wikidata: Optional[str] = None