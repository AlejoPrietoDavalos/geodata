from abc import ABC
from functools import cached_property

from pymongo import MongoClient
from pymongo.database import Database

from geodata.csc.urls import UrlsCSC
from geodata.csc.downloads import download_csv
from geodata.db.colls.countries import CountriesColl
from geodata.db.colls.states import StatesColl
from geodata.db.colls.cities import CitiesColl

__all__ = ["WorldDataDB", "WORLD_DATA", "COUNTRIES", "STATES", "CITIES"]


WORLD_DATA = "world_data"
COUNTRIES = "countries"
STATES = "states"
CITIES = "cities"

class BaseWorldDataDB(ABC):
    def __init__(self, mongo_client: MongoClient, db_name: str = WORLD_DATA):
        self._client = mongo_client
        self._db = self.client[db_name]
    
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
    def __init__(self, mongo_client: MongoClient, db_name: str = WORLD_DATA):
        super().__init__(mongo_client=mongo_client, db_name=db_name)

    def set_unique_keys(self) -> None:
        self.countries.coll.create_index([(self.countries.column_id_csc, 1)], unique=True)
        self.states.coll.create_index([(self.states.column_id_csc, 1)], unique=True)
        self.cities.coll.create_index([(self.cities.column_id_csc, 1)], unique=True)

    def download_csc(self, verbose: bool = True) -> None:
        print("~"*40)
        print(f"{'countries':~^40}")
        df_countries = download_csv(url=UrlsCSC.countries)
        self.countries.process_df_csc(df_countries, verbose=verbose)

        print("~"*40)
        print("~"*40)
        print(f"{'states':~^40}")
        df_states = download_csv(url=UrlsCSC.states)
        self.states.process_df_csc(df_states, verbose=verbose)

        print("~"*40)
        print("~"*40)
        print(f"{'cities':~^40}")
        df_cities = download_csv(url=UrlsCSC.cities)
        self.cities.process_df_csc(df_cities, verbose=verbose)