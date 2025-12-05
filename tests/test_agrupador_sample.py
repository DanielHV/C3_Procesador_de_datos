import sys
import os
import pandas as pd
import numpy as np

# asegurar que la raíz del proyecto esté en sys.path para que los tests puedan importar el paquete local
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from agrupador.aggregator import aggregate_from_dfs


def test_categorical_count_and_numeric_mean():
    df = pd.DataFrame({
        'region': ['A', 'A', 'B', 'B', 'B'],
        'sexo': ['M', 'F', 'M', 'M', 'F'],
        'edad': [20, 30, 40, 50, 60]
    })

    meta = pd.DataFrame([
        {'id': 'region', 'alias': 'region', 'catalogo': '', 'historial': ''},
        {'id': 'sexo', 'alias': 'sexo', 'catalogo': '{"M":"Masculino","F":"Femenino"}', 'historial': ''},
        {'id': 'edad', 'alias': 'edad', 'catalogo': '', 'historial': ''}
    ])

    config = {
        'meta_columns': {'id': 'id', 'alias': 'alias', 'catalog': 'catalogo', 'history': 'historial'},
        'transformations': [
            {'name': 'sexo_counts', 'select': {'type': 'list', 'values': ['sexo']}, 'operation': 'count'},
            {'name': 'edad_mean', 'select': {'type': 'list', 'values': ['edad']}, 'operation': 'mean'}
        ]
    }

    out_df, out_meta = aggregate_from_dfs(df, meta, config, ['region'])

    assert set(out_df['region'].unique()) == {'A', 'B'}
    count_cols = [c for c in out_df.columns if 'sexo__cat(' in c and '__count' in c]
    assert len(count_cols) >= 2
    assert any('edad__mean' in c for c in out_df.columns)
    assert not out_meta.empty
    assert 'path' in out_meta.columns
    derived_ids = out_meta[out_meta['id'].str.contains(r'sexo__cat\(')]['id'].tolist()
    assert derived_ids, 'expected derived metadata rows for sexo counts'
    sample_id = derived_ids[0]
    hist_val = out_meta.loc[out_meta['id'] == sample_id, 'historial'].iloc[0]
    path_val = out_meta.loc[out_meta['id'] == sample_id, 'path'].iloc[0]
    src_name = sample_id.split('__', 1)[0]
    expected_path = f"{hist_val};{src_name}" if hist_val else src_name
    assert path_val == expected_path
