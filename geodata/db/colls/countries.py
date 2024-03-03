from pymongo.collection import Collection

from geodata.db.colls.base import BaseRegionColl
from geodata.db.models.country import Country

class CountriesColl(BaseRegionColl):
    def __init__(self, coll: Collection):
        super().__init__(coll=coll)
    
    @property
    def name_singular(self) -> str:
        return "country"
    
    @property
    def cls_coll(self) -> Country:
        return Country