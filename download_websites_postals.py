from typing import Literal

from geodata.db.client import WorldDataDB

def main(mode: Literal["all", "only_empty"] = "all", max_workers: int = 5, verbose: bool = True):
    db = WorldDataDB()
    db.download_websites_postals(mode=mode, max_workers=max_workers, verbose=verbose)

if __name__ == "__main__":
    MODE = "all"        # If 'all' only 
    MAX_WORKERS = 5
    VERBOSE = True      # Can redirect to .log file.
    main(mode=MODE, max_workers=MAX_WORKERS, verbose=VERBOSE)