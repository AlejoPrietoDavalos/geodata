from typing import Tuple

from SPARQLWrapper import SPARQLWrapper, JSON

from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

def url_wikidata_sparql() -> str:
    return "https://query.wikidata.org/sparql"

def get_sparql() -> SPARQLWrapper:
    return SPARQLWrapper(url_wikidata_sparql())


def search_country_wikidata_id(country: Country) -> Tuple[int, str | None]:
    """
    Function to find the Wikidata ID of a country given its name and country code.
    :return: id_wikidata of the country if found, None otherwise.
    """
    sparql = get_sparql()
    query = f"""
    SELECT ?country ?countryLabel WHERE {{
        ?country wdt:P31 wd:Q6256; # Instance of country.
                wdt:P297 "{country.country_code}"; # Country code ISO 3166-1 alpha-2.
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    LIMIT 1
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    bindings = results["results"]["bindings"]
    country_id_wikidata = None if len(bindings) == 0 else bindings[0]["country"]["value"].split('/')[-1]
    return country.country_id_csc, country_id_wikidata


def search_state_wikidata_id(state: State) -> Tuple[int, str | None]:
    """
    Function to find the Wikidata ID of a state given its name and country code.
    :return: id_wikidata of the state if found, None otherwise.
    """
    sparql = get_sparql()
    query = f"""
    SELECT ?place ?placeLabel WHERE {{
        ?place wdt:P31/wdt:P279* wd:Q10864048; # Instance or Subclass of administrative territorial entity.
            rdfs:label "{state.state_name}"@en;
            wdt:P17 ?country. # Country of the place.
        ?country wdt:P297 "{state.country_code}". # Country code ISO 3166-1 alpha-2.
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    LIMIT 1
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    bindings = results["results"]["bindings"]
    state_id_wikidata = None if len(bindings) == 0 else bindings[0]["place"]["value"].split('/')[-1]
    return state.state_id_csc, state_id_wikidata


def search_city_wikidata_id(city: City) -> Tuple[int, str | None]:
    """
    Function to find the Wikidata ID of a place given its name and country code.
    :return: id_wikidata of the place if found, None otherwise.
    """
    sparql = get_sparql()
    query = f"""
    SELECT ?place ?placeLabel WHERE {{
        ?place wdt:P31/wdt:P279* wd:Q486972; # Instance or Subclass of human as settlement.
            rdfs:label "{city.city_name}"@en;
            wdt:P17 ?country. # País del lugar
        ?country wdt:P297 "{city.country_code}". # Country code ISO 3166-1 alpha-2.
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    LIMIT 1
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    bindings = results["results"]["bindings"]
    city_id_wikidata = None if len(bindings) == 0 else bindings[0]["place"]["value"].split('/')[-1]
    return city.city_id_csc, city_id_wikidata