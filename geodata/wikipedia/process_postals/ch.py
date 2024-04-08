from typing import List

from geodata.wikipedia.process_postals.common import (
    is_type_1, process_type_1,
    is_type_2, process_type_2
)

def categorize_postal_code_ch(postal_code_dirty: str) -> str | None:
    if is_type_1(postal_code_dirty):
        return "TYPE_1"
    elif is_type_2(postal_code_dirty):
        return "TYPE_2"
    else:
        return None

def process_postal_code_ch(postal_code_dirty: str) -> List[str] | None:
    category = categorize_postal_code_ch(postal_code_dirty)
    
    if category == "TYPE_1":
        return process_type_1(postal_code_dirty)
    elif category == "TYPE_2":
        return process_type_2(postal_code_dirty)
    else:
        return None