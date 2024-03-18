from geodata.db.client import WorldDataDB

if __name__ == "__main__":
    db = WorldDataDB()
    db.set_unique_keys()