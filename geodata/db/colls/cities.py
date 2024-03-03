from pymongo.collection import Collection

from geodata.db.colls.base import BaseRegionColl
from geodata.db.models.city import City

class CitiesColl(BaseRegionColl):
    def __init__(self, coll: Collection):
        super().__init__(coll=coll)
    
    @property
    def name_singular(self) -> str:
        return "city"
    
    @property
    def cls_coll(self) -> City:
        return City
