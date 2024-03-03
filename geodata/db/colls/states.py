from pymongo.collection import Collection

from geodata.db.colls.base import BaseRegionColl

class StatesColl(BaseRegionColl):
    def __init__(self, coll: Collection):
        super().__init__(coll=coll)
    
    @property
    def name_singular(self) -> str:
        return "state"