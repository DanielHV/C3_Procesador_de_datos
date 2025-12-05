"""Conversor: convierte variables numéricas en categorías y produce datos de bins (bin data).

Funciones principales exportadas:
- convert_from_files(input_csv, meta_csv, config_json, group_by, out_csv)
- convert_from_dfs(data_df, meta_df, config_dict, group_by)

Configuración esperada (parte relevante):
{
    "meta_columns": { "id":"NOMBRE DE VARIABLE", "alias":"DESCRIPCIÓN DE VARIABLE", "catalog":"CATALOGOS", "path":"OBSERVACIÓN" },
    "transformations": [
    { "select": { ... }, "operation": "qcut"|"cut", "params" : { "q": 4, "bins": [...], "labels": [...], "normalize_by": "_group_count" } }
  ]
}

Salida: CSV con columnas: name, code, bin, interval, cells, path.
"""
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np
import re
from conversor.config import load_config, select_columns_from_metadata
from conversor.metadata import get_alias_for_var


def _concat_group_id(row: pd.Series, group_by: List[str], sep: str = '') -> str:
    """Concatena las columnas de `group_by` en un identificador único de grupo."""
    parts = []
    for c in group_by:
        val = row.get(c, '')
        if pd.isna(val):
            parts.append('')
        else:
            parts.append(str(val))
    return sep.join(parts)


def convert_from_dfs(data_df: pd.DataFrame, meta_df: pd.DataFrame, config: Dict[str, Any], group_by: List[str]) -> pd.DataFrame:
    """Construye el catálogo de bins a partir de data_df y meta_df según config.

    Retorna un DataFrame con columnas: name, code, bin, interval, cells
    """
    meta_cols = config.get('meta_columns', {})
    id_col = meta_cols.get('id', 'id')
    alias_col = meta_cols.get('alias', 'alias')
    path_col = meta_cols.get('path', 'path')
    path_sep = config.get('path_separator', ';')

    transformations = config.get('transformations', [])

    # asegura que las columnas de agrupamiento se traten como cadenas para preservar el formato
    if group_by:
        for g in group_by:
            if g in data_df.columns:
                data_df[g] = data_df[g].astype(object).where(pd.notna(data_df[g]), '')
                data_df[g] = data_df[g].astype(str)


    # lista de filas para el resultado (bin data)
    rows = []

    for trans in transformations:
        selector = trans.get('select', {})
        selected = select_columns_from_metadata(meta_df, selector, id_col, alias_col)
        operation = trans.get('operation')
        # parámetros operacionales deben venir de `params`.
        params = trans.get('params', {}) or {}
        normalize_by = params.get('normalize_by')

        for col in selected:
            # resolver alias -> id real si es necesario
            if col not in data_df.columns:
                alias_match = meta_df[meta_df[alias_col] == col]
                if not alias_match.empty:
                    col_real = alias_match.iloc[0][id_col]
                else:
                    continue
            else:
                col_real = col

            # Evitar categorizar columnas que son identificadoras
            # o la columna interna `_group_count`
            if col_real in (group_by or []) or str(col_real) == '_group_count':
                continue

            # calcular la discretización sobre la variable completa (sin agrupar)
            # intentar convertir a numérico similar al comportamiento de `agrupador`
            series = data_df[col_real]
            if not pd.api.types.is_numeric_dtype(series):
                conv = pd.to_numeric(series, errors='coerce')
                orig_non_na = series.dropna().shape[0]
                conv_non_na = conv.dropna().shape[0]
                if orig_non_na == 0:
                    series_used = conv
                elif conv_non_na == orig_non_na:
                    series_used = conv
                else:
                    raise ValueError(f"La operación '{operation}' requiere datos numéricos en la columna '{col_real}'; la conversión a numérico falló para algunos valores")
            else:
                series_used = series

            temp = data_df.copy()
            temp[col_real] = series_used

            # si se solicita `normalize_by`, normalizar el valor por fila antes de categorizar.
            # divisiones por cero o NaN producirán NaN (comportamiento por defecto de pandas).
            if normalize_by:
                if normalize_by in temp.columns:
                    temp[col_real] = temp[col_real].astype(float) / temp[normalize_by].astype(float)
                else:
                    raise ValueError(f"normalize_by='{normalize_by}' solicitado pero la columna no se encontró en los datos")

            values = temp[col_real]

            if operation == 'qcut':
                q = params.get('q', 4)
                try:
                    cats = pd.qcut(values, q=q, duplicates='drop')
                except Exception:
                    cats = pd.qcut(values.rank(method='first'), q=q, duplicates='drop')
            elif operation == 'cut':
                bins = params.get('bins')
                labels = params.get('labels')
                if bins is None:
                    raise ValueError(f"Transform '{trans.get('name','trans')}': 'bins' debe proporcionarse dentro de 'params' para la operación 'cut'.")
                cats = pd.cut(values, bins=bins, labels=labels)
            else:
                continue

            # conservar las columnas originales para poder construir ids desde `group_by` si se solicita
            gv = temp.reset_index()
            gv['_cat'] = cats.values

            cat_series = gv.groupby('_cat', observed=False)
            categories = list(cat_series.groups.keys())
            categories = [c for c in categories if pd.notna(c)] + ([np.nan] if any(pd.isna(categories)) else [])

            for idx, cat in enumerate([c for c in categories if pd.notna(c)], start=1):
                rows_for_cat = gv[gv['_cat'] == cat]
                # construir identificadores de celda: si todas las columnas de group_by existen,
                # concatenarlas, en caso contrario usar el índice de fila
                if group_by and all((g in temp.columns) for g in group_by):
                    ids = rows_for_cat.apply(lambda r: _concat_group_id(r, group_by, sep=''), axis=1).tolist()
                else:
                    ids = rows_for_cat['index'].astype(str).tolist()
                cells = '{' + ','.join(ids) + '}'
                # estandarizar el separador medio entre extremos a ':' sin espacios
                try:
                    interval = re.sub(r",\s*", ":", str(cat))
                except Exception:
                    interval = str(cat)
                code = get_alias_for_var(meta_df, id_col, alias_col, col_real)
                # reflejar la normalización en el nombre y alias de la variable de salida
                if normalize_by:
                    name_out = f"{col_real}__norm({normalize_by})"
                    code_out = f"{code}__norm({normalize_by})"
                else:
                    name_out = col_real
                    code_out = code
                # obtener el valor de 'path' desde metadata
                path_val = ""
                if id_col in meta_df.columns and col_real in meta_df[id_col].astype(str).tolist():
                    path_val = meta_df[meta_df[id_col].astype(str) == str(col_real)].iloc[0].get(path_col, "")
                elif alias_col in meta_df.columns and col_real in meta_df[alias_col].astype(str).tolist():
                    path_val = meta_df[meta_df[alias_col].astype(str) == str(col_real)].iloc[0].get(path_col, "")
                # añadir el nombre de la variable categorizada al path usando separador configurable
                if path_val is None:
                    path_val = ""
                path_val = (path_val + path_sep + col_real) if path_val != "" else col_real

                out_row = {
                    'name': name_out,
                    'code': code_out,
                    'bin': idx,
                    'interval': interval,
                    'cells': cells,
                    'count': len(ids),
                    'path': path_val,
                }

                rows.append(out_row)

    out_df = pd.DataFrame(rows)
    return out_df


def convert_from_files(input_csv: str, meta_csv: str, config_json: Any, group_by: List[str], out_csv: str) -> str:
    conf = load_config(config_json) if isinstance(config_json, str) else config_json
    data_df = pd.read_csv(input_csv, dtype=str)
    meta_df = pd.read_csv(meta_csv, dtype=str).fillna("")
    out_df = convert_from_dfs(data_df, meta_df, conf, group_by)
    out_df.to_csv(out_csv, index=False)
    return out_csv
