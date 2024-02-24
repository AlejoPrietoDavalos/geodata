from pymongo.collection import Collection

from geodata.db.colls.base import BaseRegionColl

class CountysColl(BaseRegionColl):
    def __init__(self, coll: Collection):
        super().__init__(coll=coll)