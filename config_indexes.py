from pymongo import MongoClient

from geodata.db.client import WorldDataDB

if __name__ == "__main__":
    mongo_client = MongoClient()
    db = WorldDataDB(mongo_client=mongo_client)
    db.set_unique_keys()