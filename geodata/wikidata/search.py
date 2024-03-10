from typing import Tuple, List

from SPARQLWrapper import JSON

from geodata.wikidata.sparql import get_sparql
from geodata.wikidata._etc import _raise_model_error
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City


def query_country_wikidata_id(country: Country) -> str:
    return f"""
        SELECT ?country ?countryLabel WHERE {{
            ?country wdt:P31 wd:Q6256;
                    wdt:P297 "{country.country_code}";
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        LIMIT 1
        """

def query_state_wikidata_id(state: State) -> str:
    return f"""
        SELECT ?place ?placeLabel WHERE {{
            ?place wdt:P31/wdt:P279* wd:Q10864048;
                rdfs:label "{state.state_name}"@en;
                wdt:P17 ?country. # Country of the place.
            ?country wdt:P297 "{state.country_code}".
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        LIMIT 1
        """

def query_city_wikidata_id(city: City) -> str:
    return f"""
        SELECT ?place ?placeLabel WHERE {{
            ?place wdt:P31/wdt:P279* wd:Q486972;
                rdfs:label "{city.city_name}"@en;
                wdt:P17 ?country. # País del lugar
            ?country wdt:P297 "{city.country_code}".
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        LIMIT 1
        """


def query_from_model(model: Country | State | City) -> str:
    if isinstance(model, Country):
        query = query_country_wikidata_id(model)
    elif isinstance(model, State):
        query = query_state_wikidata_id(model)
    elif isinstance(model, City):
        query = query_city_wikidata_id(model)
    else:
        _raise_model_error()
    return query

def _get_model_name(model: Country | State | City) -> str:
    """ The API brings the field `country` for countries, and `place` for what we call `state` and `city`."""
    if isinstance(model, Country):
        _model_name = "country"
    elif isinstance(model, State) or isinstance(model, State):
        _model_name = "place"
    else:
        _raise_model_error()
    return _model_name

def _id_wikidata_from_results(results, model: Country | State | City) -> str | None:
    bindings = results["results"]["bindings"]
    MODEL_NAME = _get_model_name(model)
    id_wikidata = None if len(bindings) == 0 else bindings[0][MODEL_NAME]["value"].split('/')[-1]
    return id_wikidata

def search_id_wikidata(model: Country | State | City) -> Tuple[int, str | None]:
    query = query_from_model(model)
    sparql = get_sparql()
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()
    id_wikidata = _id_wikidata_from_results(results=results, model=model)
    return model.id_csc, id_wikidata


def search_websites_and_postal_codes(id_wikidata: str | None) -> Tuple[List[str], List[str]]:
    if id_wikidata is None:
        return [], []
    sparql = get_sparql()
    query = f"""
    SELECT ?website ?postalCode WHERE {{
      OPTIONAL {{ wd:{id_wikidata} wdt:P856 ?website. }}
      OPTIONAL {{ wd:{id_wikidata} wdt:P281 ?postalCode. }}
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    websites = []
    postal_codes = []
    for binding in results["results"]["bindings"]:
        if "website" in binding:
            websites.append(binding["website"]["value"])
        if "postalCode" in binding:
            postal_codes.append(binding["postalCode"]["value"])

    return websites, postal_codes



