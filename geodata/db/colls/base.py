from typing import Type, Tuple, List, Literal, Generator
from abc import ABC, abstractproperty
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

from pymongo.results import UpdateResult
from pymongo.collection import Collection
from pymongo.cursor import Cursor
import numpy as np

from geodata.wikidata.search import (
    search_country_wikidata_id, search_state_wikidata_id,
    search_city_wikidata_id, search_websites_and_postal_codes
)
from geodata.db.models.base import GeoZoneModel
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

DEFAULT_WORKERS = 10

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

    def update_websites_postal_codes(self, id_csc: int, websites: List[str], postal_codes: List[str]) -> UpdateResult:
        result = self.coll.update_one(
            {self.column_id_csc: int(id_csc)},
            {"$set": {"websites_wikidata": websites, "postal_codes_wikidata": postal_codes}}
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

    def search_all_none_id_wikidata(self, max_workers: int = DEFAULT_WORKERS) -> None:
        num_docs = sum(1 for _ in self.find_id_wikidata_none())

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_id_wikidata, self.cls_coll(**doc)) for doc in self.find_id_wikidata_none())
            
            for i, future in enumerate(as_completed(iter_futures), start=1):
                id_csc, id_wikidata = future.result()
                print(f"{i}/{num_docs}")

    def search_websites_and_postal_codes(
            self,
            model: Country | State | City,
            mode: Literal["all", "only_empty"] = "only_empty"
        ) -> Tuple[List[str], List[str]]:
        if mode == "all":
            websites, postal_codes = search_websites_and_postal_codes(model.id_wikidata)
        elif mode == "only_empty":
            if len(model.websites_wikidata) == 0 or len(model.postal_codes_wikidata) == 0:
                websites, postal_codes = search_websites_and_postal_codes(model.id_wikidata)
            else:
                websites, postal_codes = [], []
        else:
            raise ValueError("`mode` incorrect.")
        
        websites = list(np.unique([w for w in websites if w not in model.websites_wikidata]))
        postal_codes = list(np.unique([p for p in postal_codes if p not in model.postal_codes_wikidata]))

        if len(websites) != 0 or len(postal_codes) != 0:
            model.websites_wikidata.extend(websites)
            model.postal_codes_wikidata.extend(postal_codes)
            self.update_websites_postal_codes(model.id_csc, model.websites_wikidata, model.postal_codes_wikidata)
            print(f"id_csc={model.id_csc} | websites={model.websites_wikidata} | postal_codes_wikidata={model.postal_codes_wikidata}")
        return websites, postal_codes

    def search_all_websites_and_postal_codes(
            self,
            mode: Literal["all", "only_empty"] = "only_empty",
            max_workers: int = DEFAULT_WORKERS
        ) -> None:
        if mode not in ["all", "only_empty"]:
            raise ValueError("`mode` incorrect.")
        
        num_docs = sum(1 for _ in self.coll.find({}))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_websites_and_postal_codes, self.cls_coll(**doc), mode) for doc in self.coll.find({}))
            for i, future in enumerate(as_completed(iter_futures), start=1):
                websites, postal_codes = future.result()
                print(f"{i}/{num_docs}")

    def random_docs(self, size: int = 1) -> List[dict]:
        return list(self.coll.aggregate([{"$sample": {"size": size}}]))