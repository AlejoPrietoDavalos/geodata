from pymongo import MongoClient

from geodata.db.client import WorldDataDB

def main(verbose: bool = True):
    mongo_client = MongoClient()
    db = WorldDataDB(mongo_client=mongo_client)
    db.download_csc(verbose=verbose)

if __name__ == "__main__":
    VERBOSE = True      # Can redirect to .log file.
    main(verbose=VERBOSE)