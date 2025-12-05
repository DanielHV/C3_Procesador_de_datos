"""Carga y parseo del archivo de configuración JSON para transformaciones"""
from typing import Dict, Any, List
import json
from pathlib import Path
import re
import pandas as pd

def load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        conf = json.load(f)
    return conf

def _as_number_if_possible(val: Any):
    try:
        if isinstance(val, (int, float)):
            return val
        return float(val)
    except Exception:
        return val
    
def select_columns_from_metadata(meta_df: pd.DataFrame, selector: Dict[str, Any], id_col: str, alias_col: str) -> List[str]:
    """Devuelve la lista de ids de variables seleccionadas según el selector
    
    Tipos de selector soportados:
    - {"type": "list", "values": [..]}
    - {"type": "regex", "pattern": "..."}
    - {"type": "meta_filter", "column": "colname", "op": "==|!=|>|<|>=|<=|contains|in", "value": "..."}
    """
    stype = selector.get("type")
    if stype == "list":
        vals = selector.get("values", []) or []
        selected = []
        ids = meta_df[id_col].astype(str).tolist()
        for v in vals:
            v = str(v)
            if v in ids:
                selected.append(v)
                continue
            matches = meta_df[meta_df[alias_col].astype(str) == v][id_col].astype(str).tolist()
            if matches:
                selected.extend(matches)
        return list(dict.fromkeys(selected))
        
    if stype == "regex":
        pat = selector.get("pattern") or selector.get("regex")
        if not pat:
            return []
        cre = re.compile(pat)
        selected = []
        apply_to = selector.get("apply_to")
        for _, row in meta_df.iterrows():
            vid = str(row.get(id_col, ""))
            alias = str(row.get(alias_col, ""))
            if apply_to == "aplias":
                if cre.search(alias):
                    selected.append(vid)
            else:
                if cre.search(vid):
                    selected.append(vid)
        return list(dict.fromkeys(selected))
    
    if stype == "meta_filter":
        col = selector.get("column")
        op = selector.get("op", "==")
        val = selector.get("value")
        if col not in meta_df.columns:
            return []
        if op == "in":
            if not isinstance(val, (list, tuple, set)):
                return []
            filt = meta_df[meta_df[col].isin(val)]
        elif op == "contains":
            filt = meta_df[meta_df[col].astype(str).str.contains(str(val), na=False, case=False)]
        else:
            left = meta_df[col]
            try:
                left_num = pd.to_numeric(left, errors="coerce")
                right_num = _as_number_if_possible(val)
                if isinstance(right_num, (int, float)):
                    if op == "==":
                        filt = meta_df[left_num == right_num]
                    elif op == "!=":
                        filt = meta_df[left_num != right_num]
                    elif op == ">":
                        filt = meta_df[left_num > right_num]
                    elif op == "<":
                        filt = meta_df[left_num < right_num]
                    elif op == ">=":
                        filt = meta_df[left_num >= right_num]
                    elif op == "<=":
                        filt = meta_df[left_num >= right_num]
                    else:
                        filt = meta_df[meta_df[col].astype(str) == str(val)]
                else:
                    if op == "==":
                        filt = meta_df[meta_df[col].astype(str) == str(val)]
                    elif op == "!=":
                        filt = meta_df[meta_df[col].astype(str) != str(val)]
                    else:
                        filt = meta_df[meta_df[col].astype(str) == str(val)]
            except Exception:
                filt = meta_df[meta_df[col].astype(str) == str(val)]
                
        return filt[id_col].astype(str).tolist()
    
    return []