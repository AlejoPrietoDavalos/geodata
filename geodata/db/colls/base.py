from abc import ABC, abstractproperty

from pymongo.results import UpdateResult
from pymongo.collection import Collection
from pymongo.cursor import Cursor

class BaseRegionColl(ABC):
    def __init__(self, coll: Collection):
        self._coll = coll
    
    @abstractproperty
    def name_singular(self) -> str:
        """ Name of the collection in singular."""
        ...
    
    @property
    def coll(self) -> Collection:
        return self._coll
    
    @property
    def column_id_wikidata(self) -> str:
        return f"{self.name_singular}_id_wikidata"
    
    @property
    def column_id_csc(self) -> str:
        return f"{self.name_singular}_id_csc"
    
    def find_id_wikidata_none(self) -> Cursor:
        return self.coll.find({self.column_id_wikidata: None})

    def update_id_wikidata(self, id_csc: int, id_wikidata: str) -> UpdateResult:
        result = self.coll.update_one(
            {self.column_id_csc: int(id_csc)},
            {"$set": {self.column_id_wikidata: id_wikidata}}
        )
        return result