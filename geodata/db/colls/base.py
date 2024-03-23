from typing import Type, Tuple, List, Literal, Generator
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

from geodata.wikidata.search import (
    search_id_wikidata,
    search_websites_and_postal_codes,
    country_code_to_lang,
    search_name_native,
    search_name_english
)
from geodata.db.models.base import GeoZoneModel
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

DEFAULT_WORKERS = 5
COUNTRY_CODE = "country_code"
CREATED_TIME = "created_time"
UPDATED_TIME = "updated_time"
WEBSITES_WIKIDATA = "websites_wikidata"
POSTAL_CODES_WIKIDATA = "postal_codes_wikidata"

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

    @property
    def column_name_native(self) -> str:
        return f"{self.name_singular}_name_native"

    @property
    def column_name_english(self) -> str:
        return f"{self.name_singular}_name_english"

    def iter_models(self) -> Generator[Country | State | City, None, None]:
        return (self.cls_coll(**model_doc) for model_doc in self.coll.find({}))


    def find_id_wikidata_none(self) -> Cursor:
        return self.coll.find({self.column_id_wikidata: None})

    def add_updated_time_to_set(self, dict2set: dict) -> dict:
        current_time = datetime.now(tz=UTC)
        dict2set[UPDATED_TIME] = current_time
        return dict2set

    def update_one_by_id_csc(self, id_csc: int, dict2set: dict) -> UpdateResult:
        return self.coll.update_one({self.column_id_csc: int(id_csc)}, {"$set": dict2set})

    def update_id_wikidata(self, id_csc: int, id_wikidata: str) -> UpdateResult:
        dict2set = {self.column_id_wikidata: id_wikidata}
        dict2set = self.add_updated_time_to_set(dict2set)
        result = self.update_one_by_id_csc(id_csc=id_csc, dict2set=dict2set)
        return result

    def update_websites_postal_codes(self, id_csc: int, websites: List[str], postal_codes: List[str]) -> UpdateResult:
        dict2set = {WEBSITES_WIKIDATA: websites, POSTAL_CODES_WIKIDATA: postal_codes}
        dict2set = self.add_updated_time_to_set(dict2set)
        result = self.update_one_by_id_csc(id_csc=id_csc, dict2set=dict2set)
        return result

    def update_name_native_and_english(self, id_csc: int, name_native: str | None, name_english: str | None) -> UpdateResult:
        assert name_native is not None or name_english is not None, "They cannot be both None at the same time."
        
        dict2set = {}
        if name_native is not None:
            dict2set[self.column_name_native] = name_native
        if name_english is not None:
            dict2set[self.column_name_english] = name_english
        
        dict2set = self.add_updated_time_to_set(dict2set)
        result = self.update_one_by_id_csc(id_csc=id_csc, dict2set=dict2set)
        return result

    def search_id_wikidata(self, model: Country | State | City, verbose: bool = True) -> Tuple[int, str | None]:
        executed_complete = False
        tries = 0
        while not executed_complete:
            try:
                id_csc, id_wikidata = search_id_wikidata(model)
                executed_complete = True
            except Exception as e:
                print("~"*40)
                print(f"ERROR - {model.name} - TIMEOUT 60 SECONDS")
                print("~"*40)
                tries += 1
                if tries == 3:
                    return model.id_csc, None
                time.sleep(60)

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

    def _search_websites_and_postal_codes(
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

    def search_websites_and_postal_codes(
            self,
            model: Country | State | City,
            mode: Literal["all", "only_empty"] = "all",
            verbose: bool = True
        ) -> Tuple[List[str], List[str]]:
        executed_complete = False
        tries = 0
        while not executed_complete:
            try:
                websites, postal_codes = self._search_websites_and_postal_codes(model=model, mode=mode, verbose=verbose)
                executed_complete = True
            except Exception as e:
                print("~"*40)
                print(f"ERROR - {model.name} - TIMEOUT 60 SECONDS")
                #print(f"{e}")
                print("~"*40)
                tries += 1
                if tries == 3:
                    return [], []
                time.sleep(60)
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

    def search_name_native_and_english(self, model: Country | State | City, verbose: bool = True) -> None:
        lang = country_code_to_lang(model.country_code)
        if model.id_wikidata is not None and lang != "":
            name_native = search_name_native(model.id_wikidata, lang) if model.name_native is None else None
        else:
            name_native = None
        
        time.sleep(1)

        if model.id_wikidata is not None:
            name_english = search_name_english(model.id_wikidata) if model.name_english is None else None
        else:
            name_english = None
        
        if name_native is not None or name_english is not None:
            result = self.update_name_native_and_english(
                id_csc = model.id_csc,
                name_native = name_native,
                name_english = name_english
            )

            if verbose:
                msg = [f"id_csc={model.id_csc}"]
                if name_native is not None:
                    msg.append(f"name_native={name_native}")
                if name_english is not None:
                    msg.append(f"name_english={name_english}")
                msg = " | ".join(msg)
                print(msg)

    def search_all_name_native_and_english(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True) -> None:
        num_docs = sum(1 for _ in self.coll.find({}))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_name_native_and_english, model, verbose) for model in self.iter_models())
            for i, future in enumerate(as_completed(iter_futures), start=1):
                future.result()
                if verbose:
                    print(f"{i}/{num_docs}")