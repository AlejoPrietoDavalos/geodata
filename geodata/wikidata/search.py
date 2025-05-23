from typing import Tuple, List
import time

from SPARQLWrapper import QueryResult
from tenacity import retry, stop_after_attempt, wait_fixed
TENACITY_WAIT = 60
TENACITY_STOP = 3

from geodata.wikidata.querys import (
    query_country_id_wikidata,
    query_state_id_wikidata,
    query_state_id_wikidata_lang,
    query_city_id_wikidata,
    query_city_id_wikidata_lang,
    query_websites_and_postal_codes,
    query_name_native,
    query_name_english
)
from geodata.wikidata.lang import country_code_to_lang
from geodata.wikidata.sparql import results_from_query
from geodata.wikidata._etc import _raise_model_error
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

def country2lang(country_code: str) -> str:
    return country_code_to_lang(country_code)

def _get_model_name(model: Country | State | City) -> str:
    """ The API brings the field `country` for countries, and `place` for what we call `state` and `city`."""
    if isinstance(model, Country):
        _model_name = "country"
    elif isinstance(model, State) or isinstance(model, City):
        _model_name = "place"
    else:
        _raise_model_error()
    return _model_name

def _id_wikidata_from_results(results: QueryResult.ConvertResult, model: Country | State | City) -> str | None:
    bindings = results["results"]["bindings"]
    MODEL_NAME = _get_model_name(model)
    id_wikidata = None if len(bindings) == 0 else bindings[0][MODEL_NAME]["value"].split('/')[-1]
    return id_wikidata

def process_query_search_id_wikidata(query: str, model: Country | State | City) -> Tuple[int, str | None]:
    results = results_from_query(query=query)
    id_wikidata = _id_wikidata_from_results(results=results, model=model)
    return model.id_csc, id_wikidata


def search_id_wikidata(model: Country | State | City) -> Tuple[int, str | None]:
    lang = country2lang(model.country_code)
    if isinstance(model, Country):
        query = query_country_id_wikidata(model)
        id_csc, id_wikidata = process_query_search_id_wikidata(query, model)
    
    elif isinstance(model, State):
        query = query_state_id_wikidata(model)
        id_csc, id_wikidata = process_query_search_id_wikidata(query, model)
        if id_wikidata is None and lang != "":
            query = query_state_id_wikidata_lang(model, lang)
            id_csc, id_wikidata =  process_query_search_id_wikidata(query, model)
    
    elif isinstance(model, City):
        query = query_city_id_wikidata(model)
        id_csc, id_wikidata = process_query_search_id_wikidata(query, model)
        if id_wikidata is None and lang != "":
            query = query_city_id_wikidata_lang(model, lang)
            id_csc, id_wikidata =  process_query_search_id_wikidata(query, model)
    else:
        _raise_model_error()
    return id_csc, id_wikidata


def search_websites_and_postal_codes(id_wikidata: str | None) -> Tuple[List[str], List[str]]:
    if id_wikidata is None:
        return [], []
    query = query_websites_and_postal_codes(id_wikidata)
    results = results_from_query(query=query)

    websites = []
    postal_codes = []
    for binding in results["results"]["bindings"]:
        if "website" in binding:
            websites.append(binding["website"]["value"])
        if "postalCode" in binding:
            postal_codes.append(binding["postalCode"]["value"])

    return websites, postal_codes


@retry(stop=stop_after_attempt(TENACITY_STOP), wait=wait_fixed(TENACITY_WAIT))
def search_name_native(id_wikidata: str | None, lang: str) -> str | None:
    if lang == "" or id_wikidata is None:
        return None
    query = query_name_native(id_wikidata, lang)
    results = results_from_query(query=query)
    if len(results["results"]["bindings"]) > 0:
        name_native = results["results"]["bindings"][0]["entityLabel"]["value"]
    else:
        name_native = None
    return name_native

@retry(stop=stop_after_attempt(TENACITY_STOP), wait=wait_fixed(TENACITY_WAIT))
def search_name_english(id_wikidata: str | None) -> str | None:
    if id_wikidata is None:
        return None
    query = query_name_english(id_wikidata)
    results = results_from_query(query=query)
    if len(results["results"]["bindings"]) > 0:
        name_english = results["results"]["bindings"][0]["entityLabel"]["value"]
    else:
        name_english = None
    return name_english