import argparse

from geodata.db.client import WorldDataDB

def main(max_workers: int = 5, verbose: bool = True, with_concurrent: bool = False):
    db = WorldDataDB()
    db.download_postals_wikipedia(max_workers=max_workers, verbose=verbose, with_concurrent=with_concurrent)

if __name__ == "__main__":
    MAX_WORKERS = 5
    VERBOSE = True      # Can redirect to .log file.
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-concurrent", action="store_true")
    args = parser.parse_args()
    main(max_workers=MAX_WORKERS, verbose=VERBOSE, with_concurrent=args.with_concurrent)