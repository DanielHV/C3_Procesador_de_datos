"""Funciones auxiliares para manejar metadatos CSV."""
from typing import Tuple
import pandas as pd


def load_metadata(path: str, id_col: str, alias_col: str, catalog_col: str, path_col: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    # limpiar columnas Unnamed
    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    # aseguramos columnas existentes
    for c in [id_col, alias_col, catalog_col, path_col]:
        if c not in df.columns:
            df[c] = ""
    return df


def update_path_for_original(meta_df: pd.DataFrame, id_col: str, path_col: str, original_name: str, sep: str = ';') -> pd.DataFrame:
    """Concatena original_name al path de la variable identificada por id_col en meta_df.

    `sep` especifica el separador a usar entre piezas del path.
    """
    def _append_path(p):
        if not p or pd.isna(p):
            return str(original_name)
        return f"{p}{sep}{original_name}"

    if 'path' not in meta_df.columns:
        if path_col in meta_df.columns:
            meta_df['path'] = meta_df[path_col].astype(str)
        else:
            meta_df['path'] = ""

    mask = meta_df[id_col].astype(str) == str(original_name)
    if mask.any():
        meta_df.loc[mask, 'path'] = meta_df.loc[mask, 'path'].astype(str).apply(_append_path)

    return meta_df


def build_metadata_for_output(output_columns, source_meta_df: pd.DataFrame, id_col: str, alias_col: str, catalog_col: str, path_col: str, path_sep: str = ';'):
    """Crea un DataFrame de metadatos para las columnas de salida.

    - Si la columna de salida corresponde exactamente a una columna de entrada (ej. columna de agrupación), copia metadata y deja historial igual.
    - Si es derivada (por ejemplo 'var__cat(A)__count'), busca la variable de origen 'var' y copia alias/catalog, y concatena el nombre original al historial.
    """
    import pandas as pd
    rows = []
    src_idx = {str(r[id_col]): r for _, r in source_meta_df.iterrows()}
    alias_idx = {str(r[alias_col]): r for _, r in source_meta_df.iterrows()}

    # conservar la lista de todas las columnas del metadato de origen para poder preservar campos extra
    src_columns = list(source_meta_df.columns)

    for col in output_columns:

        # seleccionar columnas derivadas
        is_derived = "__" in col
        if is_derived:
            src = col.split("__", 1)[0]
        else:
            src = col

        src_row = src_idx.get(str(src))
        if src_row is None:
            src_row = alias_idx.get(str(src))

        if src_row is not None:

            # concatenar path previo con la pieza generada usando el separador configurable
            prev_path = src_row.get(path_col, "") or ""
            new_piece = src
            if prev_path:
                produced_path = f"{prev_path}{path_sep}{new_piece}"
            else:
                produced_path = new_piece

            # agregar sufijo
            suffix = col[len(src):] if col.startswith(src) else ''
            src_alias = src_row.get(alias_col, src)
            alias_out = f"{src_alias}{suffix}"
            row = {c: src_row.get(c, "") for c in src_columns}
            row[id_col] = col
            row[alias_col] = alias_out

            if is_derived:
                row[catalog_col] = "{}"
            else:
                row[catalog_col] = src_row.get(catalog_col, "")
            # No sobrescribir la columna configurada `path_col` — conservar el valor original.
            row[path_col] = src_row.get(path_col, "") if path_col in src_row else src_row.get(path_col, "")
            # La columna canónica 'path' contiene la concatenación producida.
            row['path'] = produced_path
        else:
            row = {c: "" for c in src_columns}
            row[id_col] = col
            row[alias_col] = col
            row[catalog_col] = "{}" if is_derived else ""
            # Para columnas sin fila de origen, dejar `path_col` vacío y usar `path` canónico con la fuente.
            row[path_col] = ""
            row['path'] = src

        rows.append(row)

    return pd.DataFrame(rows)
