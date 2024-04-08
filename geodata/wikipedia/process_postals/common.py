from typing import List
import re

def is_type_1(postal_code_dirty: str) -> bool:
    """ TYPE_1 -> `nnnnn, nnnnn, nnnnn`"""
    return re.match(r'^(\d+\s?,?\s?)+$', postal_code_dirty) is not None

def process_type_1(postal_code_dirty: str) -> List[str]:
    return [code.strip() for code in postal_code_dirty.split(',')]


def _is_left_bigger_than_right(postal_code_dirty: str) -> bool:
    postal_i, postal_f = map(int, postal_code_dirty.split('-'))
    return postal_i >= postal_f

def is_type_2(postal_code_dirty: str) -> bool:
    """ TYPE_2 -> `nnnnn-nnnnn`"""
    return re.match(r'^\d+-\d+$', postal_code_dirty) is not None \
        and postal_code_dirty.count("-") == 1 \
        and _is_left_bigger_than_right(postal_code_dirty)

def process_type_2(postal_code_dirty: str) -> List[str]:
    postal_i_str, postal_f_str = postal_code_dirty.split('-')
    postal_i_str, postal_f_str = postal_i_str.strip(), postal_f_str.strip()
    range_length = len(range(int(postal_i_str), int(postal_f_str) + 1))
    return [str(int(postal_i_str) + i).zfill(len(postal_i_str)) for i in range(range_length)]


def find_nums_exactly_digits(text: str, n_digits: int) -> list:
    return re.findall(r'\b\d{%d}\b' % n_digits, text)