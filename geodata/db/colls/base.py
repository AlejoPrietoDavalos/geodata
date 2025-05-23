from typing import Type, Tuple, List, Generator
from datetime import datetime
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

from pymongo import errors
from pymongo.results import UpdateResult
from pymongo.collection import Collection
from pymongo.cursor import Cursor
import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from geodata.wikidata.search import (
    search_websites_and_postal_codes, country_code_to_lang,
    search_id_wikidata, search_name_native, search_name_english
)
from geodata.db.models.base import (
    GeoZoneModel, OK, EXEC, STATUS, DownType, DownStatus,
    DOWN_ID_WIKIDATA, DOWN_WEBSITES_POSTALS,
    DOWN_NAME_NATIVE_ENGLISH, DOWN_POSTALS_WIKIPEDIA
)
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City
from geodata.wikipedia.postal_wikipedia import get_postal_codes_from_wikipedia
from geodata.wikipedia.process_postals.utils import postprocess_postal_codes_wikipedia
from geodata.utils_time import UTC

DEFAULT_WORKERS = 5
COUNTRY_CODE = "country_code"
CREATED_TIME = "created_time"
UPDATED_TIME = "updated_time"
WEBSITES_WIKIDATA = "websites_wikidata"
POSTAL_CODES_WIKIDATA = "postal_codes_wikidata"
POSTAL_CODES_WIKIPEDIA = "postal_codes_wikipedia"
POSTAL_CODES_WIKIPEDIA_CLEAN = "postal_codes_wikipedia_clean"

TENACITY_WAIT = 60
TENACITY_STOP = 3


def datetime_now_str() -> str:
    return datetime.now(tz=UTC).strftime("%Y_%m_%d_%H_%M_%S")

class BaseRegionColl(ABC):
    def __init__(self, coll: Collection):
        self._coll = coll
    
    @property
    @abstractmethod
    def name_singular(self) -> str:
        """ Name of the collection in singular."""
        ...
    
    @property
    @abstractmethod
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
    
    def status_key(self, *, down_type: DownType) -> str:
        return f"{STATUS}.{down_type}"

    def filter_status(self, *, down_type: DownType, down_status: DownStatus) -> dict:
        return {self.status_key(down_type=down_type): down_status}
    
    def is_status_exec(self, *, down_type: DownType) -> bool:
        filter_status_exec = self.filter_status(down_type=down_type, down_status=EXEC)
        return self.coll.count_documents(filter_status_exec) != 0
    
    def update_one_status(self, id_csc: int, down_type: DownType, down_status: DownStatus) -> None:
        self.coll.update_one({self.column_id_csc: id_csc}, {"$set": {self.status_key(down_type=down_type): down_status}})
    
    def iter_models(self, search_dict: dict = None) -> Generator[Country | State | City, None, None]:
        if search_dict is None:
            search_dict = {}
        return (self.cls_coll(**model_doc) for model_doc in self.coll.find(search_dict))

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

    def update_postal_codes_wikipedia(self, id_csc: int, postal_codes: List[str]) -> UpdateResult:
        dict2set = {POSTAL_CODES_WIKIPEDIA: postal_codes}
        dict2set = self.add_updated_time_to_set(dict2set)
        result = self.update_one_by_id_csc(id_csc=id_csc, dict2set=dict2set)
        return result
    
    def update_postal_codes_wikipedia_clean(self, id_csc: int, postal_codes_clean: List[str]) -> UpdateResult:
        dict2set = {POSTAL_CODES_WIKIPEDIA_CLEAN: postal_codes_clean}
        dict2set = self.add_updated_time_to_set(dict2set)
        result = self.update_one_by_id_csc(id_csc=id_csc, dict2set=dict2set)
        return result

    @retry(stop=stop_after_attempt(TENACITY_STOP), wait=wait_fixed(TENACITY_WAIT))
    def _search_id_wikidata(self, model: Country | State | City, verbose: bool = True) -> None:
        id_csc, id_wikidata = search_id_wikidata(model)
        if id_wikidata is not None:
            self.update_id_wikidata(id_csc, id_wikidata)
            if verbose:
                print(f"Updated: {self.column_id_csc}={id_csc} | {self.column_id_wikidata}={id_wikidata}")

    def search_id_wikidata(self, model: Country | State | City, verbose: bool = True) -> None:
        try:
            self._search_id_wikidata(model=model, verbose=verbose)
            self.update_one_status(id_csc=model.id_csc, down_type=DOWN_ID_WIKIDATA, down_status=OK)
        except Exception as e:
            print(e)

    def get_filter_models(self, *, filter_: dict, down_type: DownType) -> dict:
        filter_models = filter_.copy()
        status_key = self.status_key(down_type=down_type)

        filter_status_exec = self.filter_status(down_type=down_type, down_status=EXEC)
        if self.coll.count_documents(filter_status_exec) != 0:
            return filter_status_exec
        else:
            self.coll.update_many(filter_models, {"$set": {status_key: EXEC}})
            return filter_models

    def search_all_none_id_wikidata(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        filter_models = self.get_filter_models(filter_={self.column_id_wikidata: None}, down_type=DOWN_ID_WIKIDATA)
        models = list(self.iter_models(filter_models))
        num_docs = len(models)

        if with_concurrent:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                iter_futures = (pool.submit(self.search_id_wikidata, model, verbose) for model in models)
                
                for i, future in enumerate(as_completed(iter_futures), start=1):
                    future.result()
                    if verbose:
                        print(f"{i}/{num_docs}")
        else:
            i = 1
            while models:
                model = models.pop(0)
                self.search_id_wikidata(model, verbose)
                if verbose:
                    print(f"{i}/{num_docs}")
                i += 1

    @retry(stop=stop_after_attempt(TENACITY_STOP), wait=wait_fixed(TENACITY_WAIT))
    def _search_websites_and_postal_codes(self, model: Country | State | City, verbose: bool = True) -> None:
        websites, postal_codes = search_websites_and_postal_codes(id_wikidata=model.id_wikidata)
        websites = list(np.unique([w for w in websites if w not in model.websites_wikidata]))
        postal_codes = list(np.unique([p for p in postal_codes if p not in model.postal_codes_wikidata]))

        if len(websites) != 0 or len(postal_codes) != 0:
            model.websites_wikidata.extend(websites)
            model.postal_codes_wikidata.extend(postal_codes)
            self.update_websites_postal_codes(model.id_csc, model.websites_wikidata, model.postal_codes_wikidata)
            if verbose:
                print(f"id_csc={model.id_csc} | websites_wikidata={model.websites_wikidata} | postal_codes_wikidata={model.postal_codes_wikidata}")

    def search_websites_and_postal_codes(self, model: Country | State | City, verbose: bool = True) -> None:
        try:
            self._search_websites_and_postal_codes(model=model, verbose=verbose)
            self.update_one_status(id_csc=model.id_csc, down_type=DOWN_WEBSITES_POSTALS, down_status=OK)
        except Exception as e:
            print(e)
    
    def search_all_websites_and_postal_codes(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        filter_models = self.get_filter_models(filter_={}, down_type=DOWN_WEBSITES_POSTALS)
        models = list(self.iter_models(filter_models))
        num_docs = len(models)

        if with_concurrent:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                iter_futures = (pool.submit(self.search_websites_and_postal_codes, model, verbose) for model in models)
                for i, future in enumerate(as_completed(iter_futures), start=1):
                    future.result()
                    if verbose:
                        print(f"{i}/{num_docs}")
        else:
            i = 1
            while models:
                model = models.pop(0)
                self.search_websites_and_postal_codes(model=model, verbose=verbose)
                if verbose:
                    print(f"{i}/{num_docs}")
                i += 1

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
                self.coll.update_one({self.column_id_csc: model_new.id_csc}, {"$set": update_fields})
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
        is_all_exec_ok = True
        lang = country_code_to_lang(model.country_code)
        if model.id_wikidata is not None and lang != "":
            try:
                name_native = search_name_native(model.id_wikidata, lang) if model.name_native is None else None
            except:
                name_native = None
                is_all_exec_ok = False
        else:
            name_native = None
        
        time.sleep(1)

        if model.id_wikidata is not None:
            try:
                name_english = search_name_english(model.id_wikidata) if model.name_english is None else None
            except:
                name_english = None
                is_all_exec_ok = False
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
        if is_all_exec_ok:
            self.update_one_status(id_csc=model.id_csc, down_type=DOWN_NAME_NATIVE_ENGLISH, down_status=OK)

    def search_all_name_native_and_english(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        _f = {"$or": [{self.column_name_native: None}, {self.column_name_english: None}]}
        filter_models = self.get_filter_models(filter_=_f, down_type=DOWN_NAME_NATIVE_ENGLISH)
        models = list(self.iter_models(filter_models))
        num_docs = len(models)
        
        if with_concurrent:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                iter_futures = (pool.submit(self.search_name_native_and_english, model, verbose) for model in models)
                for i, future in enumerate(as_completed(iter_futures), start=1):
                    future.result()
                    if verbose:
                        print(f"{i}/{num_docs}")
        else:
            i = 1
            while models:
                model = models.pop(0)
                self.search_name_native_and_english(model=model, verbose=verbose)
                if verbose:
                    print(f"{i}/{num_docs}")
                i += 1

    def _postprocess_postal_codes_wikipedia(self, model: State | City, verbose: bool = True) -> None:
        postal_codes_clean = postprocess_postal_codes_wikipedia(model=model)
        
        is_updated = False
        for postal in postal_codes_clean:
            if postal not in model.postal_codes_wikipedia_clean:
                model.postal_codes_wikipedia_clean.append(postal)
                is_updated = True
        
        if is_updated:
            self.update_postal_codes_wikipedia_clean(model.id_csc, model.postal_codes_wikipedia_clean)
            if verbose:
                print(f"id_csc={model.id_csc} | postal_codes_wikipedia_clean={model.postal_codes_wikipedia_clean}")

    def postprocess_postal_codes_wikipedia(self, model: State | City, verbose: bool = True) -> None:
        try:
            self._postprocess_postal_codes_wikipedia(model=model, verbose=verbose)
        except Exception as e:
            print(e)

    def postprocess_all_postal_codes_wikipedia(self, verbose: bool = True) -> None:
        models = list(self.iter_models())
        num_docs = len(models)

        i = 1
        while models:
            model = models.pop(0)
            self.postprocess_postal_codes_wikipedia(model=model, verbose=verbose)
            print(f"{i}/{num_docs}")
            i += 1

    @retry(stop=stop_after_attempt(TENACITY_STOP), wait=wait_fixed(TENACITY_WAIT))
    def _search_postals_wikipedia(self, model: Country | State | City, verbose: bool = True) -> None:
        postal_codes = get_postal_codes_from_wikipedia(id_wikidata=model.id_wikidata)
        postal_codes = list(np.unique([p for p in postal_codes if p not in model.postal_codes_wikipedia]))
        
        if len(postal_codes) != 0:
            model.postal_codes_wikipedia.extend(postal_codes)
            self.update_postal_codes_wikipedia(model.id_csc, model.postal_codes_wikipedia)
            if verbose:
                print(f"id_csc={model.id_csc} | postal_codes_wikipedia={model.postal_codes_wikipedia}")

    def search_postals_wikipedia(self, model: State | City, verbose: bool = True) -> None:
        try:
            self._search_postals_wikipedia(model=model, verbose=verbose)
            self.update_one_status(id_csc=model.id_csc, down_type=DOWN_POSTALS_WIKIPEDIA, down_status=OK)
        except Exception as e:
            print(e)

    def search_all_postals_wikipedia(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        filter_models = self.get_filter_models(filter_={}, down_type=DOWN_POSTALS_WIKIPEDIA)
        models = list(self.iter_models(filter_models))
        num_docs = len(models)
        
        if with_concurrent:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                iter_futures = (pool.submit(self.search_postals_wikipedia, model, verbose) for model in models)
                for i, future in enumerate(as_completed(iter_futures), start=1):
                    future.result()
                    if verbose:
                        print(f"{i}/{num_docs}")
        else:
            i = 1
            while models:
                model = models.pop(0)
                self.search_postals_wikipedia(model=model, verbose=verbose)
                if verbose:
                    print(f"{i}/{num_docs}")
                i += 1