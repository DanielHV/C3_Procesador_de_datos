import pytest
import src.verify as verify
import pandas as pd
import os
from dotenv import load_dotenv
import ast
import json


DATAFRAME_ALIAS_PATH = os.getenv("ruta_csv_dataframe_alias")
CSV_OUTPUT_PATH = os.getenv("ruta_csv_salida")
COLUMN_ALIAS_NAME = os.getenv("columna_dataframe_alias_nombres")
COLUMN_ALIAS = os.getenv("columna_dataframe_alias_alias")
COLUMN_ALIAS_DESCRIPTION = os.getenv("columna_dataframe_alias_descripcion")

GRID_CSV_PATH = json.loads(os.getenv("rutas_csv_mallas"))
GRID_CSV_MUN_PATH = GRID_CSV_PATH["mun"]

grid_df = pd.read_csv(GRID_CSV_MUN_PATH)
alias_df = pd.read_csv(DATAFRAME_ALIAS_PATH)


###VERIFY ALL FILES AVAILABLE

def test_dataframe_alias_file_exists():
    assert DATAFRAME_ALIAS_PATH is not None, "ruta_csv_dataframe_alias is not set"
    assert os.path.exists(DATAFRAME_ALIAS_PATH), f"File not found: {DATAFRAME_ALIAS_PATH}"

def test_mallas_mun_file_exists():
    assert GRID_CSV_MUN_PATH is not None, "rutas_csv_mallas.mun is not set"
    assert os.path.exists(GRID_CSV_MUN_PATH), f"File not found: {GRID_CSV_MUN_PATH}"

### VERIFY INTEGRITY OF CSV

def test_dataframe_alias_has_column_nombres():
    assert COLUMN_ALIAS_NAME in alias_df.columns, f"Column '{COLUMN_ALIAS_NAME}' not found"

def test_dataframe_alias_has_column_alias():
    assert COLUMN_ALIAS in alias_df.columns, f"Column '{COLUMN_ALIAS}' not found"

def test_dataframe_alias_has_column_descripcion():
    assert COLUMN_ALIAS_DESCRIPTION in alias_df.columns, f"Column '{COLUMN_ALIAS_DESCRIPTION}' not found"


### VERIFY THAT THE ROWS IN CSV EXISTS IN MUNICIPALES
# def test_alias_var_columns_exist_in_mallas():
#     expected_columns = alias_df[COLUMN_ALIAS_NAME].tolist()
#     missing = [col for col in expected_columns if col not in grid_df.columns]
#     assert not missing, f"Missing columns in mallas: {missing}"
#

### VERIFY VALUES IN RANGE
def test_alias_var_values_within_range():
    errors = []
    for _, row in alias_df.iterrows():
        column = row[COLUMN_ALIAS_NAME]
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
