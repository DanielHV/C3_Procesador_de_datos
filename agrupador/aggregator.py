"""Implementación del agregador de datos.

Funciones principales:
    - aggregate_from_files(input_csv, metadata_csv, config_json, group_by, out_csv, out_meta_csv)
    - aggregate_from_dfs(data_df, meta_df, config, group_by)

El módulo cubre conteo para categóricas
y agregaciones numéricas (mean, median, sum, std, min, max) y discretizaciones vía qcut/cut.
"""
from typing import Tuple, List, Dict, Any, Optional
import pandas as pd
import numpy as np
from .config import load_config, select_columns_from_metadata
from .metadata import build_metadata_for_output, load_metadata
import re


NUMERIC_OPS = {"mean", "median", "sum", "std", "min", "max"}
CARDINALITY_LIMIT = 100  # límite seguro para expandir categorías en columnas (evita explosion de memoria)


def _is_categorical(series: pd.Series, meta_row: Optional[Dict[str, Any]] = None) -> bool:
    if pd.api.types.is_numeric_dtype(series):
        return False
    return True


def aggregate_from_dfs(data_df: pd.DataFrame, meta_df: pd.DataFrame, config: Dict[str, Any], group_by: List[str], ignore_conversion_errors: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Agrupa data_df según group_by y aplica transformaciones definidas en config.

    Retorna (aggregated_df, new_meta_df)
    """
    meta_cols = config.get("meta_columns", {})
    id_col = meta_cols.get("id", "id")
    alias_col = meta_cols.get("alias", "alias")
    catalog_col = meta_cols.get("catalog", "catalogo")
    path_col = meta_cols.get("path", "path")

    if group_by:
        for g in group_by:
            if g in data_df.columns:
                # reemplazar NaN por cadena vacía primero para evitar el literal 'nan' tras astype(str)
                data_df[g] = data_df[g].where(pd.notna(data_df[g]), "")
                data_df[g] = data_df[g].astype(str)

    grouped = data_df.groupby(group_by)
    out = pd.DataFrame(index=grouped.size().index)
    # convertir el índice en columnas (claves de grupo)
    out = out.reset_index(drop=True)
    # calcular las agregaciones por grupo y construir un DataFrame indexado por group_by
    group_keys = list(group_by)
    agg_base = grouped.size().reset_index(name="_group_count")
    result_df = agg_base.copy()

    out_columns_meta = []

    transformations = config.get("transformations", [])
    for trans in transformations:
        name = trans.get("name", "trans")
        selector = trans.get("select", {})
        selected = select_columns_from_metadata(meta_df, selector, id_col, alias_col)
        operation = trans.get("operation")
        # parámetros operacionales
        params = trans.get("params", {}) or {}

        for col in selected:
            if col not in data_df.columns:
                # intentar por alias
                # mirar en meta_df alias
                alias_match = meta_df[meta_df[alias_col] == col]
                if not alias_match.empty:
                    col_real = alias_match.iloc[0][id_col]
                else:
                    continue
            else:
                col_real = col

            # si la columna seleccionada es también una de las claves de agrupación se salta
            if col_real in group_keys:
                continue

            series = data_df[col_real]
            op = trans.get("operation")

            # si la operación requiere datos numéricos, intentar convertir columnas que contienen números como strings a tipo numérico
            coerced_series = None
            skip_column = False
            if op in NUMERIC_OPS or op == "qcut" or op == "cut":
                if not pd.api.types.is_numeric_dtype(series):
                    conv = pd.to_numeric(series, errors='coerce')
                    orig_non_na = series.dropna().shape[0]
                    conv_non_na = conv.dropna().shape[0]
                    # si no hay valores originales no nulos, aceptamos la serie convertida (vacía)
                    if orig_non_na == 0:
                        coerced_series = conv
                    # si todos los no-nulos se convirtieron correctamente, usar la conversión
                    elif conv_non_na == orig_non_na:
                        coerced_series = conv
                    # si algunos (pero no todos) se convirtieron, también usar la conversión (no convertibles -> NaN)
                    elif conv_non_na > 0:
                        coerced_series = conv
                        print(f"Advertencia: La columna '{col_real}' contiene algunos valores no numéricos; se tratarán como NaN para la operación '{op}'")
                    # si ninguno se pudo convertir (pero había valores), decidir según flag
                    else:
                        if ignore_conversion_errors:
                            print(f"Advertencia: La columna '{col_real}' no tiene valores numéricos convertibles; se omite la transformación '{op}'")
                            skip_column = True
                        else:
                            raise ValueError(f"La operación '{op}' requiere datos numéricos en la columna '{col_real}'")

            # decidir qué serie usar para las comprobaciones de tipo y operaciones
            # si se decidió saltar esta columna (por flag), pasar a la siguiente
            if skip_column:
                continue

            series_for_check = coerced_series if coerced_series is not None else series

            if _is_categorical(series_for_check):
                # conteo por grupo y por categoría
                temp = data_df.copy()
                # Normalizar: forzar a str, recortar espacios y mapear cadenas vacías a '<missing>'
                temp[col_real] = temp[col_real].astype(str).str.strip()
                # después del strip, las cadenas que quedaron vacías representan datos faltantes/espacios
                temp[col_real] = temp[col_real].replace("", "<missing>")

                # seguridad: evitar explotar la memoria si la columna tiene demasiadas categorias
                uniq = temp[col_real].nunique(dropna=True)
                if uniq > CARDINALITY_LIMIT:
                    print(f"Advertencia: columna '{col_real}' tiene alta cardinalidad ({uniq}) > {CARDINALITY_LIMIT}; se omite para evitar explosion de memoria")
                    continue

                # conteo por grupo y por categoría usando los valores normalizados
                grouped_temp = temp.groupby(group_keys)
                vc = grouped_temp[col_real].value_counts().unstack(fill_value=0)

                # renombrar columnas como `{col}__cat('{cat}')__count`
                # se añaden comillas simples alrededor de la categoría para preservar separadores/espacios
                newcols = {c: f"{col_real}__cat('{c}')__count" for c in vc.columns}
                vc = vc.rename(columns=newcols).reset_index()
                # fusionar con result_df por group_by
                result_df = result_df.merge(vc, on=group_keys, how="left")
                out_columns_meta.extend(list(newcols.values()))
                
            else:
                # numéricas y discretizaciones
                op = operation
                if op in NUMERIC_OPS:
                    # realizar agregación
                    if coerced_series is not None:
                        temp = data_df.copy()
                        temp[col_real] = coerced_series
                    else:
                        temp = data_df.copy()

                    grouped_temp = temp.groupby(group_keys)
                    agg = grouped_temp[col_real].agg(op).reset_index().rename(columns={col_real: f"{col_real}__{op}"})
                    result_df = result_df.merge(agg, on=group_keys, how="left")
                    out_columns_meta.append(f"{col_real}__{op}")
                elif op == "qcut":
                    # tomar parámetros desde params (por ejemplo 'q')
                    q = params.get("q", 4)
                    # validar q
                    try:
                        q_int = int(q)
                    except Exception:
                        raise ValueError(f"El parámetro 'q' de qcut debe ser un entero para la transformación '{name}' en la columna '{col_real}'")
                    if q_int < 2:
                        raise ValueError(f"El parámetro 'q' de qcut debe ser >= 2 para la transformación '{name}' en la columna '{col_real}'")
                    q = q_int
                    # categorización por cuantiles por cada grupo y conteo
                    # se calcula qcut sobre una tabla temporal que puede incluir la serie convertida
                    temp = data_df.copy()
                    if coerced_series is not None:
                        temp[col_real] = coerced_series

                    try:
                        temp['_qcat'] = pd.qcut(temp[col_real], q=q, duplicates='drop')
                        vc = temp.groupby(group_keys)['_qcat'].value_counts().unstack(fill_value=0)
                    except Exception:
                        # fallback: usar qcut global basado en rangos
                        global_cat = pd.qcut(data_df[col_real].rank(method='first'), q=q, duplicates='drop')
                        temp2 = data_df.copy()
                        temp2['_qcat'] = global_cat
                        vc = temp2.groupby(group_keys)['_qcat'].value_counts().unstack(fill_value=0)

                    # limpiar texto de intervalos en los nombres de columna: usar ':' como separador sin espacios
                    newcols = {}
                    for c in vc.columns:
                        try:
                            label = re.sub(r",\s*", ":", str(c))
                        except Exception:
                            label = str(c)
                        newcols[c] = f"{col_real}__qcut__{label}"
                    vc = vc.rename(columns=newcols).reset_index()
                    result_df = result_df.merge(vc, on=group_keys, how="left")
                    out_columns_meta.extend(list(newcols.values()))
                elif op == "cut":
                    # tomar bins/labels desde params
                    bins = params.get("bins")
                    labels = params.get("labels")
                    temp = data_df.copy()
                    if coerced_series is not None:
                        temp[col_real] = coerced_series

                    # validar bins/labels
                    if bins is None:
                        raise ValueError(f"La operación 'cut' requiere 'bins' en params para la transformación '{name}' en la columna '{col_real}'")
                    if labels is not None:
                        try:
                            if len(labels) != (len(bins) - 1):
                                raise ValueError(f"La longitud de 'labels' para cut debe ser len(bins)-1 para la transformación '{name}' en la columna '{col_real}'")
                        except TypeError:
                            # bins may be int; then labels must be None
                            pass

                    temp['_bcat'] = pd.cut(temp[col_real], bins=bins, labels=labels)
                    vc = temp.groupby(group_keys)['_bcat'].value_counts().unstack(fill_value=0)
                    # limpiar texto de intervalos en los nombres de columna: usar ':' como separador sin espacios
                    newcols = {}
                    for c in vc.columns:
                        try:
                            label = re.sub(r",\s*", ":", str(c))
                        except Exception:
                            label = str(c)
                        newcols[c] = f"{col_real}__cut__{label}"
                    vc = vc.rename(columns=newcols).reset_index()
                    result_df = result_df.merge(vc, on=group_keys, how="left")
                    out_columns_meta.extend(list(newcols.values()))
                else:
                    continue

    # limpiar columnas NaN a 0 en conteos
    # identificar columnas de conteo (categorías), qcut y cut para rellenar NaN con 0
    count_cols = [c for c in result_df.columns if "__cat(" in c or "__qcut__" in c or "__cut__" in c]
    result_df[count_cols] = result_df[count_cols].fillna(0)

    # construir metadata para las columnas de salida
    output_columns = list(result_df.columns)
    path_sep = config.get('path_separator', ';')
    new_meta = build_metadata_for_output(output_columns, meta_df, id_col, alias_col, catalog_col, path_col, path_sep)

    return result_df, new_meta


def aggregate_from_files(input_csv: str, metadata_csv: str, config_json: str, group_by: List[str], out_csv: str, out_meta_csv: str, ignore_conversion_errors: bool = False) -> Tuple[str, str]:
    conf = load_config(config_json) if isinstance(config_json, str) else config_json
    # leer datos entrantes forzando strings para que las columnas de agrupación preserven el formato
    data_df = pd.read_csv(input_csv, dtype=str).fillna("")
    # cargar metadatos usando la utilidad (esto limpia columnas 'Unnamed' que provienen de índices CSV)
    meta_cols = conf.get("meta_columns", {})
    id_col = meta_cols.get("id", "id")
    alias_col = meta_cols.get("alias", "alias")
    catalog_col = meta_cols.get("catalog", "catalogo")
    path_col = meta_cols.get("path", "path")
    meta_df = load_metadata(metadata_csv, id_col, alias_col, catalog_col, path_col)
    agg_df, new_meta_df = aggregate_from_dfs(data_df, meta_df, conf, group_by, ignore_conversion_errors=ignore_conversion_errors)
    agg_df.to_csv(out_csv, index=False)
    new_meta_df.to_csv(out_meta_csv, index=False)
    return out_csv, out_meta_csv

