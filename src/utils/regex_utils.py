import pandas as pd
import re

def obtener_variables_regex_df(df: pd.DataFrame, regex: str) -> list:
    if not isinstance(df, pd.DataFrame):
        raise TypeError('El parámetro df debe ser de tipo pd.DataFrame')
    if not isinstance(regex, str):
        raise TypeError('El parámetro regex debe ser de tipo str')
    try:
        pattern = re.compile(regex)
    except re.error:
        raise ValueError(f'La expresión regular {regex} no es válida')
    return list(df.filter(regex=regex).columns)