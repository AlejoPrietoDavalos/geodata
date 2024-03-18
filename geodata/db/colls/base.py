from typing import Type, Tuple, List, Literal
from datetime import datetime, UTC
from abc import ABC, abstractproperty
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

from pymongo import errors
from pymongo.results import UpdateResult
from pymongo.collection import Collection
from pymongo.cursor import Cursor
import numpy as np
import pandas as pd

from geodata.wikidata.search import search_id_wikidata, search_websites_and_postal_codes
from geodata.db.models.base import GeoZoneModel
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

DEFAULT_WORKERS = 5
COUNTRY_CODE = "country_code"
CREATED_TIME = "created_time"
UPDATED_TIME = "updated_time"


def datetime_now_str() -> str:
    return datetime.now(tz=UTC).strftime("%Y_%m_%d_%H_%M_%S")

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
        current_time = datetime.now(tz=UTC)
        result = self.coll.update_one(
            {self.column_id_csc: int(id_csc)},
            {"$set": {self.column_id_wikidata: id_wikidata, UPDATED_TIME: current_time}}
        )
        return result

    def update_websites_postal_codes(self, id_csc: int, websites: List[str], postal_codes: List[str]) -> UpdateResult:
        current_time = datetime.now(tz=UTC)
        result = self.coll.update_one(
            {self.column_id_csc: int(id_csc)},
            {"$set": {"websites_wikidata": websites, "postal_codes_wikidata": postal_codes, UPDATED_TIME: current_time}}
        )
        return result

    def search_id_wikidata(self, model: Country | State | City, verbose: bool = True) -> Tuple[int, str | None]:
        id_csc, id_wikidata = search_id_wikidata(model)

        if id_wikidata is not None:
            self.update_id_wikidata(id_csc, id_wikidata)
            if verbose:
                print(f"Updated: {self.column_id_csc}={id_csc} | {self.column_id_wikidata}={id_wikidata}")
        return id_csc, id_wikidata

    def search_all_none_id_wikidata(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True) -> None:
        num_docs = sum(1 for _ in self.find_id_wikidata_none())

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_id_wikidata, self.cls_coll(**doc), verbose) for doc in self.find_id_wikidata_none())
            
            for i, future in enumerate(as_completed(iter_futures), start=1):
                id_csc, id_wikidata = future.result()
                if verbose:
                    print(f"{i}/{num_docs}")

    def search_websites_and_postal_codes(
            self,
            model: Country | State | City,
            mode: Literal["all", "only_empty"] = "all",
            verbose: bool = True
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
            if verbose:
                print(f"id_csc={model.id_csc} | websites_wikidata={model.websites_wikidata} | postal_codes_wikidata={model.postal_codes_wikidata}")
        return websites, postal_codes

    def search_all_websites_and_postal_codes(
            self,
            mode: Literal["all", "only_empty"] = "all",
            max_workers: int = DEFAULT_WORKERS,
            verbose: bool = True
        ) -> None:
        if mode not in ["all", "only_empty"]:
            raise ValueError("`mode` incorrect.")
        
        num_docs = sum(1 for _ in self.coll.find({}))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_websites_and_postal_codes, self.cls_coll(**doc), mode, verbose) for doc in self.coll.find({}))
            for i, future in enumerate(as_completed(iter_futures), start=1):
                websites, postal_codes = future.result()
                if verbose:
                    print(f"{i}/{num_docs}")

    def random_docs(self, size: int = 1) -> List[dict]:
        return list(self.coll.aggregate([{"$sample": {"size": size}}]))

    def upsert_model_csc(self, model_new: Country | State | City) -> bool:
        """ Return True if is a new document."""
        doc_old = self.coll.find_one({self.column_id_csc: model_new.id_csc})
        doc_new = model_new.model_dump()
        
        if doc_old is None:
            is_new = True
            return is_new
        else:
            update_fields = {}
            for k,v in doc_new.items():
                if k not in doc_old or (doc_old[k] is None and v is not None):
                    update_fields[k] = v
            
            if update_fields:
                update_fields[UPDATED_TIME] = doc_new[UPDATED_TIME]
                self.coll.update_one({self.column_id_csc: doc_new[model_new.id_csc]}, {"$set": update_fields})
            is_new = False
            return is_new

    def process_df_csc(self, df_csc: pd.DataFrame, verbose: bool = False) -> None:
        csc_broken = {"broken": [], "no_country_code": []}
        docs_new = []
        for i, row in df_csc.iterrows():
            row_json = json.loads(row.to_json())
            if row[COUNTRY_CODE] is not None:
                try:
                    current_time = datetime.now(tz=UTC)
                    model_new = self.cls_coll(
                        created_time = current_time,
                        updated_time = current_time,
                        **row_json
                    )
                    is_new = self.upsert_model_csc(model_new)
                    if is_new:
                        docs_new.append(model_new.model_dump())
                except Exception as e:
                    if verbose:
                        print(e)
                    csc_broken["broken"].append(row_json)
            else:
                if verbose:
                    print("no country_code")
                csc_broken["no_country_code"].append(row_json)
            
            print(f"{i+1}/{len(df_csc)}")
            if i % 2000 == 0:
                try:
                    if len(docs_new) != 0:
                        self.coll.insert_many(docs_new, ordered=False)
                        docs_new = []
                except errors.BulkWriteError as e:
                    print(e.details)
        
        try:
            if len(docs_new) != 0:
                self.coll.insert_many(docs_new, ordered=False)
                docs_new = []
        except errors.BulkWriteError as e:
            print(e.details)
        
        date_now_str = datetime_now_str()
        with open(f"{self.name_singular}_broken_{date_now_str}.json", "w") as f:
            json.dump(csc_broken, f)