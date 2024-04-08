from geodata.db.client import WorldDataDB

def main(verbose: bool = True):
    db = WorldDataDB()
    db.postprocess_postals_wikipedia(verbose=verbose)

if __name__ == "__main__":
    VERBOSE = True      # Can redirect to .log file.
    main(verbose=VERBOSE)