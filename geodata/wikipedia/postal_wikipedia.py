from typing import List
import re
import requests

from geodata.utils import random_user_agent
from geodata.wikipedia.utils import format_str_postal_codes_wikipedia

ENTITIES = "entities"
SITELINKS = "sitelinks"
ENWIKI = "enwiki"
TITLE = "title"

def extract_postal_code_lines(content: str) -> List[str]:
    pattern = r'\|?\s*postal_code\s*=\s*([^|\n]*)(?=\n|\|)'
    matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
    matches = [format_str_postal_codes_wikipedia(m.strip()) for m in matches if m.strip() != ""]
    return matches

def wikipedia_title_from_id_wikidata(id_wikidata: str) -> str | None:
    url_wikidata_api = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": id_wikidata
    }
    headers = {"User-Agent": random_user_agent()}
    response = requests.get(url_wikidata_api, params=params, headers=headers)
    data = response.json()
    
    if ENTITIES in data and id_wikidata in data[ENTITIES]:
        entity = data[ENTITIES][id_wikidata]
        if SITELINKS in entity and ENWIKI in entity[SITELINKS]:
            return entity[SITELINKS][ENWIKI][TITLE]
    return None

def get_postal_codes_from_wikipedia(id_wikidata: str, verbose: bool = False) -> List[str]:
    if id_wikidata is None:
        return []
    
    wikipedia_title = wikipedia_title_from_id_wikidata(id_wikidata)
    if wikipedia_title is None:
        return []
    
    url_wikipedia_api = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": wikipedia_title,
        "rvprop": "content",
        "formatversion": 2,
        "redirects": True
    }
    headers = {"User-Agent": random_user_agent()}
    response = requests.get(url_wikipedia_api, params=params, headers=headers)
    data = response.json()
    
    if "query" in data and "pages" in data["query"]:
        page = data["query"]["pages"][0]
        if "revisions" in page and len(page["revisions"]) > 0:
            content = page["revisions"][0]["content"]
            if verbose:
                for _l in content.split("\n"):
                    print(_l)
            postal_codes = extract_postal_code_lines(content)
            return postal_codes
    return []