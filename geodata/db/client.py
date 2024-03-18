from typing import Literal
from functools import cached_property
import os
from abc import ABC
from dotenv import load_dotenv
load_dotenv()

from pymongo import MongoClient
from pymongo.database import Database

from geodata.csc.urls import UrlsCSC
from geodata.csc.downloads import download_csv
from geodata.db.colls.countries import CountriesColl
from geodata.db.colls.states import StatesColl
from geodata.db.colls.cities import CitiesColl
from geodata.db.config import Cfg

__all__ = ["WorldDataDB", "WORLD_DATA", "COUNTRIES", "STATES", "CITIES"]


WORLD_DATA = "world_data"
COUNTRIES = "countries"
STATES = "states"
CITIES = "cities"

def _get_mongo_client() -> MongoClient:
    return MongoClient(host=os.getenv("HOST"), port=int(os.getenv("PORT")))

class BaseWorldDataDB(ABC):
    def __init__(self):
        self._client = _get_mongo_client()
        self._db = self.client[os.getenv("DB_NAME")]
    
    @property
    def client(self) -> MongoClient:
        return self._client
    
    @property
    def db(self) -> Database:
        return self._db

    @cached_property
    def countries(self) -> CountriesColl:
        return CountriesColl(coll=self.db[COUNTRIES])
    
    @cached_property
    def states(self) -> StatesColl:
        return StatesColl(coll=self.db[STATES])

    @cached_property
    def cities(self) -> CitiesColl:
        return CitiesColl(coll=self.db[CITIES])


class WorldDataDB(BaseWorldDataDB):
    def __init__(self):
        super().__init__()

    def set_unique_keys(self) -> None:
        self.countries.coll.create_index(self.countries.column_id_csc, unique=True)
        self.states.coll.create_index(self.states.column_id_csc, unique=True)
        self.cities.coll.create_index(self.cities.column_id_csc, unique=True)

    def print_delimiter(self, name: str) -> None:
        print("~"*40)
        print("~"*40)
        print(f"{name:~^40}")

    def download_csc(self, verbose: bool = True) -> None:
        self.print_delimiter("countries")
        df_countries = download_csv(url=UrlsCSC.countries)
        self.countries.process_df_csc(df_countries, verbose=verbose)

        self.print_delimiter("states")
        df_states = download_csv(url=UrlsCSC.states)
        self.states.process_df_csc(df_states, verbose=verbose)

        self.print_delimiter("cities")
        df_cities = download_csv(url=UrlsCSC.cities)
        self.cities.process_df_csc(df_cities, verbose=verbose)
    
    def download_id_wikidata(self, max_workers: int = 5, verbose: bool = True) -> None:
        self.print_delimiter("countries")
        self.countries.search_all_none_id_wikidata(max_workers=max_workers, verbose=verbose)

        self.print_delimiter("states")
        self.states.search_all_none_id_wikidata(max_workers=max_workers, verbose=verbose)

        self.print_delimiter("cities")
        self.cities.search_all_none_id_wikidata(max_workers=max_workers, verbose=verbose)

    def download_websites_postals(
            self,
            mode: Literal["all", "only_empty"] = "all",
            max_workers: int = 5,
            verbose: bool = True
        ) -> None:
        self.print_delimiter("countries")
        self.countries.search_all_websites_and_postal_codes(mode=mode, max_workers=max_workers, verbose=verbose)

        self.print_delimiter("states")
        self.states.search_all_websites_and_postal_codes(mode=mode, max_workers=max_workers, verbose=verbose)

        self.print_delimiter("cities")
        self.cities.search_all_websites_and_postal_codes(mode=mode, max_workers=max_workers, verbose=verbose)