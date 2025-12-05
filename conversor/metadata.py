"""Utilidades para cargar y construir metadata para las salidas del conversor.

Todas las funciones devuelven o esperan DataFrames con columnas de tipo cadena
y manejan valores faltantes sustituyéndolos por cadenas vacías cuando procede.
"""
from typing import Dict, Any
import pandas as pd


def load_metadata(path: str, id_col: str = 'id', alias_col: str = 'alias', catalog_col: str = 'catalogo', path_col: str = 'path') -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    for c in [id_col, alias_col, catalog_col, path_col]:
        if c not in df.columns:
            df[c] = ""
    return df


def get_alias_for_var(meta_df: pd.DataFrame, id_col: str, alias_col: str, var: str) -> str:
    """Devuelve el alias de la variable `var` usando `meta_df`.

    Si no se encuentra, devuelve `var` tal cual.
    """
    if id_col in meta_df.columns and var in meta_df[id_col].astype(str).tolist():
        row = meta_df[meta_df[id_col].astype(str) == str(var)].iloc[0]
        return row.get(alias_col, var)
    if alias_col in meta_df.columns and var in meta_df[alias_col].astype(str).tolist():
        row = meta_df[meta_df[alias_col].astype(str) == str(var)].iloc[0]
        return row.get(alias_col, var)
    return var

