from enum import Enum

class UrlsCSC(Enum):
    cities = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv"
    countries = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/countries.csv"
    regions = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/regions.csv"
    states = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/states.csv"
    subregions = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/subregions.csv"

    @property
    def is_countries(self) -> bool:
        return self.name == "countries"
    
    @property
    def is_states(self) -> bool:
        return self.name == "states"
    
    @property
    def is_cities(self) -> bool:
        return self.name == "cities"