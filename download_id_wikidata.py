from geodata.db.client import WorldDataDB

def main(max_workers: int = 5, verbose: bool = True):
    db = WorldDataDB()
    db.download_id_wikidata(max_workers=max_workers, verbose=verbose)

if __name__ == "__main__":
    MAX_WORKERS = 5
    VERBOSE = True      # Can redirect to .log file.
    main(max_workers=MAX_WORKERS, verbose=VERBOSE)