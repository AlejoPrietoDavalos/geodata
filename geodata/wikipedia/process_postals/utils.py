from typing import Callable, List

from geodata.db.models.state import State
from geodata.db.models.city import City
from geodata.wikipedia.process_postals.de import process_postal_code_de
from geodata.wikipedia.process_postals.ch import process_postal_code_ch
from geodata.wikipedia.process_postals.at import process_postal_code_at

def fn_process_postal_code(country_code: str) -> Callable[[str], List[str]] | None:
    if country_code == "DE":
        return process_postal_code_de
    elif country_code == "CH":
        return process_postal_code_ch
    elif country_code == "AT":
        return process_postal_code_at
    else:
        return None

def postprocess_postal_codes_wikipedia(model: State | City):
    fn_process = fn_process_postal_code(model.country_code)
    if len(model.postal_codes_wikipedia) == 0 or fn_process is None:
        return []
    
    postal_codes_clean = []
    for postal_code_dirty in model.postal_codes_wikipedia:
        postals_clean = fn_process(postal_code_dirty)
        if postals_clean is not None:
            postal_codes_clean.extend(postals_clean)
    return postal_codes_clean