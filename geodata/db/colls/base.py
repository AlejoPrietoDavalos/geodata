from typing import Type, Tuple
from abc import ABC, abstractproperty
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo.results import UpdateResult
from pymongo.collection import Collection
from pymongo.cursor import Cursor

from geodata.wikidata.search import search_country_wikidata_id, search_state_wikidata_id, search_city_wikidata_id
from geodata.db.models.base import GeoZoneModel
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

class BaseRegionColl(ABC):
    def __init__(self, coll: Collection):
        self._coll = coll
    
    @abstractproperty
    def name_singular(self) -> str:
        """ Name of the collection in singular."""
        ...
    
    @abstractproperty
    def cls_coll(self) -> Type[GeoZoneModel]:
        """ Class used for the data model."""
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

    def search_id_wikidata(self, model: Country | State | City) -> Tuple[int, str | None]:
        if isinstance(model, Country):
            id_csc, id_wikidata = search_country_wikidata_id(model)
        elif isinstance(model, State):
            id_csc, id_wikidata = search_state_wikidata_id(model)
        elif isinstance(model, City):
            id_csc, id_wikidata = search_city_wikidata_id(model)
        else:
            raise ValueError("Invalid object, expected `Country`, `State` or `City`.")

        if id_wikidata is not None:
            self.update_id_wikidata(id_csc, id_wikidata)
            print(f"Updated: {self.column_id_csc}={id_csc} | {self.column_id_wikidata}={id_wikidata}")
        return id_csc, id_wikidata

    def search_all_none_id_wikidata(self, max_workers: int = 10):
        num_docs = sum(1 for _ in self.find_id_wikidata_none())

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_id_wikidata, self.cls_coll(**doc)) for doc in self.find_id_wikidata_none())
            
            for i, future in enumerate(as_completed(iter_futures), start=1):
                id_csc, id_wikidata = future.result()
                print(f"{i}/{num_docs}")
