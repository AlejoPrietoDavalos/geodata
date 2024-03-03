from typing import Dict, List
from abc import ABC
from functools import cached_property

from pymongo import MongoClient, DESCENDING, ASCENDING, UpdateOne
from pymongo.cursor import Cursor
from pymongo.results import BulkWriteResult
from pymongo.database import Database

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

    # def set_unique_keys(self) -> None:
    #     for symbol, interval in iter_all_symbol_interval():
    #         coll_name = get_collection_name(symbol, interval)
    #         collection = self.get_collection(symbol, interval)
    #         collection.collection.create_index([(TIME_OPEN, ASCENDING)], unique=True)
    #         print(f"Creación de índice para: {coll_name}")




# def filter_time_open(time_open: int) -> dict:
#     return {TIME_OPEN: time_open}
# 
# def kline_to_update_one(kline: KLine) -> UpdateOne:
#     """ Transforma el KLine en un UpdateOne para la subida a la DB."""
#     return UpdateOne(
#         filter_time_open(kline.time_open),           # Criterio de búsqueda por time_open
#         {'$setOnInsert': kline.model_dump()},   # $setOnInsert solo actualiza en caso de insertar
#         upsert = True                           # Realiza un insert si el documento no existe.
#     )

# class CollectionKLines:
#     def __init__(self, db: Database, coll_name: str):
#         self._collection = db[coll_name]
#         self._coll_name = coll_name
#     
#     @property
#     def collection(self) -> Collection:
#         return self._collection
#     
#     @property
#     def coll_name(self) -> str:
#         return self._coll_name
#     
#     def insert_klines(self, klines: List[KLine]) -> BulkWriteResult:
#         operations = [kline_to_update_one(kline) for kline in klines]
#         result = self.collection.bulk_write(operations)
#         return result
# 
#     def find_klines(self, direction=ASCENDING) -> Cursor:
#         return self.collection.find().sort(TIME_OPEN, direction)