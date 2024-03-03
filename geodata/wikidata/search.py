from typing import Tuple

from SPARQLWrapper import SPARQLWrapper, JSON

from geodata.db.models.city import City

def search_city_wikidata_id(city: City) -> Tuple[int, str | None]:
    """
    Function to find the Wikidata ID of a place given its name and country code.
    :return: id_wikidata of the place if found, None otherwise.
    """
    # URL base de la API de Wikidata
    sparql_url = "https://query.wikidata.org/sparql"
    sparql = SPARQLWrapper(sparql_url)

    # Consulta SPARQL para buscar el ID de Wikidata
    query = f"""
    SELECT ?place ?placeLabel WHERE {{
        ?place wdt:P31/wdt:P279* wd:Q486972; # Instancia de (o subclase de) asentamiento humano
            rdfs:label "{city.city_name}"@en; # Nombre del lugar en inglés
            wdt:P17 ?country. # País del lugar
        ?country wdt:P297 "{city.country_code}". # Código ISO 3166-1 alpha-2 del país
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