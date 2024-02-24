import requests
from io import StringIO

import pandas as pd

from geodata.csc.urls import UrlsCSC

def replace_empty_to_none(x):
    return None if isinstance(x, str) and x.strip() == '' else x

def download_csv(url: str | UrlsCSC, sep=",") -> pd.DataFrame:
    """ Download the raw data from the CSC github.
    - Returns a dataframe with the csv.
    """
    url = UrlsCSC(url).value
    response = requests.get(url)
    df = pd.read_csv(StringIO(response.text), sep=sep)
    df = df.where(pd.notnull(df), None)
    df = df.map(replace_empty_to_none)
    return df
