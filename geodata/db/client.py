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
from geodata.db.colls.base import DEFAULT_WORKERS
from geodata.db.models.base import (
    OK, EXEC, DOWN_ID_WIKIDATA, DOWN_WEBSITES_POSTALS,
    DOWN_NAME_NATIVE_ENGLISH, DOWN_POSTALS_WIKIPEDIA
)

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

    @property
    def countries(self) -> CountriesColl:
        return CountriesColl(coll=self.db[COUNTRIES])
    
    @property
    def states(self) -> StatesColl:
        return StatesColl(coll=self.db[STATES])

    @property
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
    
    def download_id_wikidata(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        is_countries_exec = self.countries.is_status_exec(down_type=DOWN_ID_WIKIDATA)
        is_states_exec = self.states.is_status_exec(down_type=DOWN_ID_WIKIDATA)
        is_cities_exec = self.cities.is_status_exec(down_type=DOWN_ID_WIKIDATA)
        is_exec = is_countries_exec or is_states_exec or is_cities_exec

        if is_countries_exec or not is_exec:
            self.print_delimiter("countries")
            self.countries.search_all_none_id_wikidata(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

        if is_states_exec or not is_exec:
            self.print_delimiter("states")
            self.states.search_all_none_id_wikidata(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

        if is_cities_exec or not is_exec:
            self.print_delimiter("cities")
            self.cities.search_all_none_id_wikidata(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

    def download_websites_postals(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        is_countries_exec = self.countries.is_status_exec(down_type=DOWN_WEBSITES_POSTALS)
        is_states_exec = self.states.is_status_exec(down_type=DOWN_WEBSITES_POSTALS)
        is_cities_exec = self.cities.is_status_exec(down_type=DOWN_WEBSITES_POSTALS)
        is_exec = is_countries_exec or is_states_exec or is_cities_exec

        if is_countries_exec or not is_exec:
            self.print_delimiter("countries")
            self.countries.search_all_websites_and_postal_codes(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

        if is_states_exec or not is_exec:
            self.print_delimiter("states")
            self.states.search_all_websites_and_postal_codes(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

        if is_cities_exec or not is_exec:
            self.print_delimiter("cities")
            self.cities.search_all_websites_and_postal_codes(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)
    
    def download_name_native_and_english(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        is_countries_exec = self.countries.is_status_exec(down_type=DOWN_NAME_NATIVE_ENGLISH)
        is_states_exec = self.states.is_status_exec(down_type=DOWN_NAME_NATIVE_ENGLISH)
        is_cities_exec = self.cities.is_status_exec(down_type=DOWN_NAME_NATIVE_ENGLISH)
        is_exec = is_countries_exec or is_states_exec or is_cities_exec
        
        if is_countries_exec or not is_exec:
            self.print_delimiter("countries")
            self.countries.search_all_name_native_and_english(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)
        
        if is_states_exec or not is_exec:
            self.print_delimiter("states")
            self.states.search_all_name_native_and_english(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)
        
        if is_cities_exec or not is_exec:
            self.print_delimiter("cities")
            self.cities.search_all_name_native_and_english(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

    def download_postals_wikipedia(self, max_workers: int = DEFAULT_WORKERS, verbose: bool = True, with_concurrent: bool = True) -> None:
        is_states_exec = self.states.is_status_exec(down_type=DOWN_POSTALS_WIKIPEDIA)
        is_cities_exec = self.cities.is_status_exec(down_type=DOWN_POSTALS_WIKIPEDIA)
        is_exec = is_states_exec or is_cities_exec

        if is_states_exec or not is_exec:
            self.print_delimiter("states")
            self.states.search_all_postals_wikipedia(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

        if is_cities_exec or not is_exec:
            self.print_delimiter("cities")
            self.cities.search_all_postals_wikipedia(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

    def postprocess_postals_wikipedia(self, verbose: bool = True) -> None:
        self.print_delimiter("states")
        self.states.postprocess_all_postal_codes_wikipedia(verbose=verbose)

        self.print_delimiter("cities")
        self.cities.postprocess_all_postal_codes_wikipedia(verbose=verbose)