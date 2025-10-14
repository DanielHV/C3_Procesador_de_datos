import pandas as pd
import re

def obtener_variables_regex_df(df: pd.DataFrame, regex: str) -> list:
    """
    Obtiene una lista de nombres de columnas de un DataFrame que coinciden con una expresión regular.

    Args:
        df (pd.DataFrame): DataFrame sobre el cual se buscarán las columnas.
        regex (str): Expresión regular a utilizar para filtrar los nombres de las columnas.

    Returns:
        list: Lista de nombres de columnas que coinciden con la expresión regular.

    Raises:
        TypeError: Si los parámetros no son del tipo esperado.
        ValueError: Si la expresión regular no es válida.
    """
    # validaciones df
    if not isinstance(df, pd.DataFrame):
        raise TypeError('El parámetro df debe ser de tipo pd.DataFrame')
    
    # validaciones regex
    if not isinstance(regex, str):
        raise TypeError('El parámetro regex debe ser de tipo str')
    try:
        pattern = re.compile(regex)
    except re.error:
        raise ValueError(f'La expresión regular {regex} no es válida')
    
    # aplicar funcion filter al df con regex como parametro, se toman las columnas del resultado
    return list(df.filter(regex=regex).columns)