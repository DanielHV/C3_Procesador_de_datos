import sys
import os
import pandas as pd
import numpy as np

# asegurar que la raíz del proyecto esté en sys.path para que los tests puedan importar el paquete local
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from conversor.converter import convert_from_dfs
import pytest


def test_conversor_qcut_basic():
    df = pd.DataFrame({
        'g': ['A', 'A', 'B', 'B'],
        'val': [1, 3, 7, 9]
    })
    meta = pd.DataFrame([
        {'id': 'g', 'alias': 'g', 'catalogo': '', 'historial': ''},
        {'id': 'val', 'alias': 'VAL_ALIAS', 'catalogo': '', 'historial': ''}
    ])
    config = {
        'meta_columns': {'id': 'id', 'alias': 'alias', 'catalog': 'catalogo', 'history': 'historial'},
        'transformations': [
            {'select': {'type': 'list', 'values': ['val']}, 'operation': 'qcut', 'params': {'q': 2}, 'agg': 'mean'}
        ]
    }
    out = convert_from_dfs(df, meta, config, ['g'])
    assert not out.empty
    assert set(['name', 'code', 'bin', 'interval', 'cells', 'count']).issubset(set(out.columns))
    assert all(out['code'] == 'VAL_ALIAS')


def test_conversor_cut_multi_column_group_and_cells_format():
    df = pd.DataFrame({
        'ENT': ['01', '01', '02', '02'],
        'MUN': ['001', '002', '001', '002'],
        'edad': [5, 25, 45, 70]
    })
    meta = pd.DataFrame([
        {'id': 'ENT', 'alias': 'ENT', 'catalogo': '', 'historial': ''},
        {'id': 'MUN', 'alias': 'MUN', 'catalogo': '', 'historial': ''},
        {'id': 'edad', 'alias': 'EDAD_ALIAS', 'catalogo': '', 'historial': ''}
    ])
    config = {
        'meta_columns': {'id': 'id', 'alias': 'alias', 'catalog': 'catalogo', 'history': 'historial'},
        'transformations': [
            {'select': {'type': 'list', 'values': ['edad']}, 'operation': 'cut', 'params': {'bins': [0, 18, 40, 65, 120], 'labels': ['0-17','18-39','40-64','65+']}, 'agg': 'mean'}
        ]
    }
    out = convert_from_dfs(df, meta, config, ['ENT', 'MUN'])
    assert not out.empty
    assert all(out['cells'].str.startswith('{') & out['cells'].str.endswith('}'))


def test_conversor_normalize_by_sum():
    df = pd.DataFrame({
        'g': ['A','A','B','B'],
        'val': [10, 30, 50, 70],
        'pop': [100, 200, 300, 400]
    })
    meta = pd.DataFrame([
        {'id': 'g', 'alias': 'g', 'catalogo': '', 'historial': ''},
        {'id': 'val', 'alias': 'VAL', 'catalogo': '', 'historial': ''},
        {'id': 'pop', 'alias': 'POP', 'catalogo': '', 'historial': ''}
    ])
    config = {
        'meta_columns': {'id': 'id', 'alias': 'alias', 'catalog': 'catalogo', 'history': 'historial'},
        'transformations': [
            {'select': {'type': 'list', 'values': ['val']}, 'operation': 'qcut', 'params': {'q': 2, 'normalize_by': 'pop'}, 'agg': 'mean'}
        ]
    }
    out = convert_from_dfs(df, meta, config, ['g'])
    assert 'norm' not in out.columns
    assert not out.empty


def test_conversor_non_numeric_raises():
    df = pd.DataFrame({
        'g': ['A','A','B','B'],
        'cat': ['x','y','x','y']
    })
    meta = pd.DataFrame([
        {'id': 'g', 'alias': 'g', 'catalogo': '', 'historial': ''},
        {'id': 'cat', 'alias': 'CAT', 'catalogo': '', 'historial': ''}
    ])
    config = {
        'meta_columns': {'id': 'id', 'alias': 'alias', 'catalog': 'catalogo', 'history': 'historial'},
        'transformations': [
            {'select': {'type': 'list', 'values': ['cat']}, 'operation': 'qcut', 'params': {'q': 2}, 'agg': 'mean'}
        ]
    }
    import pytest
    with pytest.raises(ValueError):
        convert_from_dfs(df, meta, config, ['g'])


def test_conversor_numeric_string_coercion():
    df = pd.DataFrame({
        'g': ['A','A','B','B'],
        'numstr': ['10','20','30','40']
    })
    meta = pd.DataFrame([
        {'id': 'g', 'alias': 'g', 'catalogo': '', 'historial': ''},
        {'id': 'numstr', 'alias': 'NUMSTR', 'catalogo': '', 'historial': ''}
    ])
    config = {
        'meta_columns': {'id': 'id', 'alias': 'alias', 'catalog': 'catalogo', 'history': 'historial'},
        'transformations': [
            {'select': {'type': 'list', 'values': ['numstr']}, 'operation': 'qcut', 'params': {'q': 2}, 'agg': 'mean'}
        ]
    }
    out = convert_from_dfs(df, meta, config, ['g'])
    assert not out.empty
    assert all(out['code'] == 'NUMSTR')

