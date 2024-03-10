from SPARQLWrapper import SPARQLWrapper, QueryResult, JSON


def url_wikidata_sparql() -> str:
    return "https://query.wikidata.org/sparql"

def get_sparql() -> SPARQLWrapper:
    return SPARQLWrapper(url_wikidata_sparql())

def results_from_query(query: str) -> QueryResult.ConvertResult:
    sparql = get_sparql()
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results: QueryResult.ConvertResult = sparql.query().convert()
    return results
