import pytest
import src.verify as verify
import pandas as pd
import os
from dotenv import load_dotenv
import ast
import json


DICCIONARIO_PATH = os.getenv("ruta_csv_diccionario_datos")
CSV_OUTPUT_PATH = os.getenv("ruta_csv_salida")
COLUMN_DICCIONARIO_NOMBRES = os.getenv("columna_diccionario_nombres")
COLUMN_DICCIONARIO_ALIAS = os.getenv("columna_diccionario_alias", "alias")
COLUMN_DICCIONARIO_DESCRIPCION = os.getenv("columna_diccionario_descripcion")

GRID_CSV_PATH = json.loads(os.getenv("rutas_csv_mallas"))
GRID_CSV_MUN_PATH = GRID_CSV_PATH["mun"]

grid_df = pd.read_csv(GRID_CSV_MUN_PATH)
diccionario_df = pd.read_csv(DICCIONARIO_PATH)


###VERIFY ALL FILES AVAILABLE

def test_diccionario_file_exists():
    assert DICCIONARIO_PATH is not None, "ruta_csv_diccionario_datos is not set"
    assert os.path.exists(DICCIONARIO_PATH), f"File not found: {DICCIONARIO_PATH}"

def test_mallas_mun_file_exists():
    assert GRID_CSV_MUN_PATH is not None, "rutas_csv_mallas.mun is not set"
    assert os.path.exists(GRID_CSV_MUN_PATH), f"File not found: {GRID_CSV_MUN_PATH}"

### VERIFY INTEGRITY OF CSV

def test_diccionario_has_column_nombres():
    assert COLUMN_DICCIONARIO_NOMBRES in diccionario_df.columns, f"Column '{COLUMN_DICCIONARIO_NOMBRES}' not found"

def test_diccionario_has_column_alias():
    assert COLUMN_DICCIONARIO_ALIAS in diccionario_df.columns, f"Column '{COLUMN_DICCIONARIO_ALIAS}' not found"

def test_diccionario_has_column_descripcion():
    assert COLUMN_DICCIONARIO_DESCRIPCION in diccionario_df.columns, f"Column '{COLUMN_DICCIONARIO_DESCRIPCION}' not found"


### VERIFY THAT THE ROWS IN CSV EXISTS IN MUNICIPALES
# def test_diccionario_var_columns_exist_in_mallas():
#     expected_columns = diccionario_df[COLUMN_DICCIONARIO_NOMBRES].tolist()
#     missing = [col for col in expected_columns if col not in grid_df.columns]
#     assert not missing, f"Missing columns in mallas: {missing}"
#

### VERIFY VALUES IN RANGE
def test_diccionario_var_values_within_range():
    errors = []
    for _, row in diccionario_df.iterrows():
        column = row[COLUMN_DICCIONARIO_NOMBRES]
        values = ast.literal_eval(row["Values"])

        if values.get("is_category") == "true":
            continue
        if column not in grid_df.columns:
            continue

        min_val = values.get("min")
        max_val = values.get("max")

        if min_val is None or max_val is None:
            continue

        invalid = grid_df[
            (pd.to_numeric(grid_df[column], errors="coerce") < min_val) |
            (pd.to_numeric(grid_df[column], errors="coerce") > max_val)
        ][column].unique()

        if len(invalid) > 0:
            errors.append(f"{column} has values outside [{min_val}, {max_val}]: {invalid.tolist()}")

    assert not errors, "\n".join(errors)
