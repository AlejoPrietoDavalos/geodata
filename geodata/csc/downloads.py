import requests
from io import StringIO

import pandas as pd

from geodata.csc.urls import UrlsCSC

def cols_delete_countries() -> list:
    return [
        "iso3", "numeric_code", "capital", "currency", "currency_name",
        "currency_symbol", "tld", "region", "subregion", "nationality",
        "timezones", "emoji", "emojiU"
    ]

def cols_rename_countries() -> dict:
    return {
        "id": "country_id_csc",
        "iso2": "country_code",
        "name": "country_name",
        "native": "country_name_native",
        "region_id": "region_id_csc",
        "subregion_id": "subregion_id_csc"
    }


def cols_delete_states() -> list:
    return ["country_name"]

def cols_rename_states() -> dict:
    return {
        "id": "state_id_csc",
        "name": "state_name",
        "country_id": "country_id_csc",
        "type": "state_type_csc"
    }


def cols_delete_cities() -> list:
    return ["state_name", "country_name"]

def cols_rename_cities() -> dict:
    return {
        "id": "city_id_csc",
        "name": "city_name",
        "state_id": "state_id_csc",
        "country_id": "country_id_csc",
        "wikiDataId": "city_id_wikidata"
    }


def replace_empty_to_none(x):
    return None if isinstance(x, str) and x.strip() == '' else x

def download_csv(url: UrlsCSC) -> pd.DataFrame:
    """ Download the raw data from the CSC github.
    - Returns a dataframe with the csv.
    """
    url = UrlsCSC(url)
    response = requests.get(url.value)
    df = pd.read_csv(StringIO(response.text), sep=",")
    df = df.where(pd.notnull(df), None)
    df = df.map(replace_empty_to_none)

    if url.is_countries:
        df.rename(cols_rename_countries(), axis=1, inplace=True)
        df.drop(cols_delete_countries(), axis=1, inplace=True)
    elif url.is_states:
        df.rename(cols_rename_states(), axis=1, inplace=True)
        df.drop(cols_delete_states(), axis=1, inplace=True)
    elif url.is_cities:
        df.rename(cols_rename_cities(), axis=1, inplace=True)
        df.drop(cols_delete_cities(), axis=1, inplace=True)
    return df
