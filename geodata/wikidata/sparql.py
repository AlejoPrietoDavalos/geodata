from SPARQLWrapper import SPARQLWrapper, JSON


def url_wikidata_sparql() -> str:
    return "https://query.wikidata.org/sparql"

def get_sparql() -> SPARQLWrapper:
    return SPARQLWrapper(url_wikidata_sparql())


