from typing import List
import re

from geodata.wikipedia.process_postals.common import (
    is_type_1, process_type_1,
    is_type_2, process_type_2
)


# def is_type_3(postal_code_dirty: str) -> bool:
#     """ TYPE_3 -> `nnnn, nnnn-nnnn, nnn`. Combination of TYPE_1 and TYPE_2."""
#     return re.match(r'^(\b\d+(\b-[^-]\d+)?\s?,?\s?)+$', postal_code_dirty) is not None
def is_type_3(postal_code_dirty: str) -> bool:
    """ TYPE_3 -> `nnnn, nnnn-nnnn, nnn`. Combination of TYPE_1 and TYPE_2."""
    return re.match(r'^(\b\d+(?:-\d+)?(?:, ?\b\d+(?:-\d+)?)*\b)$', postal_code_dirty) is not None

def process_type_3(postal_code_dirty: str) -> List[str]:
    codes = []
    for one_part_postal in postal_code_dirty.split(','):
        one_part_postal = one_part_postal.strip()
        if '-' in one_part_postal:
            codes.extend(process_type_2(one_part_postal))
        else:
            codes.extend(process_type_1(one_part_postal))
    return codes


def is_type_4(postal_code_dirty: str) -> bool:
    """ TYPE_4 -> (nr. nnnn) nnnn, nnnnn, nnnnn"""
    return re.match(r'\(nr\.\s?\d+\)\s?(\d+\s?,?\s?)+', postal_code_dirty) is not None

def process_type_4(postal_code_dirty: str) -> List[str]:
    # Ignore first element into brackets.
    return [code.strip() for code in re.findall(r'\d+', postal_code_dirty)[1:]]


def categorize_postal_code_de(postal_code_dirty: str) -> str | None:
    if is_type_1(postal_code_dirty):
        return "TYPE_1"
    elif is_type_2(postal_code_dirty):
        return "TYPE_2"
    elif is_type_3(postal_code_dirty):
        return "TYPE_3"
    elif is_type_4(postal_code_dirty):
        return "TYPE_4"
    else:
        return None

def process_postal_code_de(postal_code_dirty: str) -> List[str] | None:
    category = categorize_postal_code_de(postal_code_dirty)
    
    if category == "TYPE_1":
        return process_type_1(postal_code_dirty)
    elif category == "TYPE_2":
        return process_type_2(postal_code_dirty)
    elif category == "TYPE_3":
        return process_type_3(postal_code_dirty)
    elif category == "TYPE_4":
        return process_type_4(postal_code_dirty)
    else:
        return None