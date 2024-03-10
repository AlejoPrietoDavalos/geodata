from typing import Literal

from pymongo import MongoClient

from geodata.db.client import WorldDataDB

def main(mode: Literal["all", "only_empty"] = "all", max_workers: int = 10, verbose: bool = True):
    mongo_client = MongoClient()
    db = WorldDataDB(mongo_client=mongo_client)
    db.download_websites_postals(mode=mode, max_workers=max_workers, verbose=verbose)

if __name__ == "__main__":
    MODE = "all"        # If 'all' only 
    MAX_WORKERS = 5
    VERBOSE = True      # Can redirect to .log file.
    main(mode=MODE, max_workers=MAX_WORKERS, verbose=VERBOSE)