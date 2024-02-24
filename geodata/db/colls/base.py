from abc import ABC

from pymongo.collection import Collection

class BaseRegionColl(ABC):
    def __init__(self, coll: Collection):
        self._coll = coll
    
    @property
    def coll(self) -> Collection:
        return self._coll