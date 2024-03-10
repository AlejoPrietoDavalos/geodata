from geodata.wikidata._etc import _raise_model_error
from geodata.db.models.country import Country
from geodata.db.models.state import State
from geodata.db.models.city import City

def query_country_id_wikidata(country: Country) -> str:
    return f"""
        SELECT ?country ?countryLabel WHERE {{
            ?country wdt:P31 wd:Q6256;
                    wdt:P297 "{country.country_code}";
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        LIMIT 1
        """

def query_state_id_wikidata(state: State) -> str:
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

def query_city_id_wikidata(city: City) -> str:
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


def query_id_wikidata_from_model(model: Country | State | City) -> str:
    if isinstance(model, Country):
        query = query_country_id_wikidata(model)
    elif isinstance(model, State):
        query = query_state_id_wikidata(model)
    elif isinstance(model, City):
        query = query_city_id_wikidata(model)
    else:
        _raise_model_error()
    return query


def query_websites_and_postal_codes(id_wikidata: str) -> str:
    return f"""
        SELECT ?website ?postalCode WHERE {{
        OPTIONAL {{ wd:{id_wikidata} wdt:P856 ?website. }}
        OPTIONAL {{ wd:{id_wikidata} wdt:P281 ?postalCode. }}
        }}
        """