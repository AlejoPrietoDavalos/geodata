from pydantic import BaseModel

class GeoZoneModel(BaseModel):
    latitude: float
    longitude: float