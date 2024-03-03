from typing import Tuple

from SPARQLWrapper import SPARQLWrapper, JSON
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo.collection import Collection

from geodata.db.colls.base import BaseRegionColl
from geodata.db.models.city import City
from geodata.wikidata.search import search_city_wikidata_id

class CitiesColl(BaseRegionColl):
    def __init__(self, coll: Collection):
        super().__init__(coll=coll)
    
    @property
    def name_singular(self) -> str:
        return "city"

    def search_city_wikidata_id(self, city: City) -> Tuple[int, str | None]:
        """
        Function to find the Wikidata ID of a place given its name and country code.
        :return: id_wikidata of the place if found, None otherwise.
        """
        city_id_csc, city_id_wikidata = search_city_wikidata_id(city)
        if city_id_wikidata is not None:
            self.update_id_wikidata(city_id_csc, city_id_wikidata)
            print(f"Updated: city_id_csc={city_id_csc} | city_id_wikidata={city_id_wikidata}")
        return city_id_csc, city_id_wikidata
    
    def search_all_none_city_wikidata_id(self, max_workers: int = 10):
        num_cities = sum(1 for _ in self.find_id_wikidata_none())

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            iter_futures = (pool.submit(self.search_city_wikidata_id, City(**city_doc)) for city_doc in self.find_id_wikidata_none())
            
            for i, future in enumerate(as_completed(iter_futures), start=1):
                city_id_csc, city_id_wikidata = future.result()
                print(f"{i}/{num_cities}")