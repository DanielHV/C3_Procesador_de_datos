#!/usr/bin/env python3
"""Runner script to execute the conversor convert_from_files with a simple CLI.

Usage example (from project root):
    python run_convert.py \
        --data "COVID19MEXICO_rework_v2.csv" \
        --meta "240708 Descriptores_rework_v2.csv" \
        --config mi_config_conversor.json \
        --group-by ENTIDAD_RES \
        --out out_bin_data.csv

Defaults: config=mi_config_conversor.json, group_by=ENTIDAD_RES, out=out_bin_data.csv
"""
import argparse
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Run conversor on dataset + metadata using a config JSON")
    p.add_argument("--data", required=True, help="Ruta al archivo de datos CSV")
    p.add_argument("--meta", required=True, help="Ruta al archivo de metadatos CSV")
    p.add_argument("--config", required=True, help="Ruta al archivo de configuración JSON")
    p.add_argument("--group-by", required=True, help="Columnas usadas para identificar los datos (separar con comas)")
    p.add_argument("--out", default="out_bin_data.csv", help="Ruta de salida de datos transformados en archivo CSV")
    args = p.parse_args()

    data_path = Path(args.data)
    meta_path = Path(args.meta)
    config_path = Path(args.config)

    for f in (data_path, meta_path, config_path):
        if not f.exists():
            print(f"ERROR: archivo no encontrado: {f}")
            sys.exit(2)

    group_by = [g.strip() for g in args.group_by.split(",") if g.strip()]
    if not group_by:
        print("ERROR: --group-by debe especificar al menos una columna")
        sys.exit(2)

    try:
        from conversor import convert_from_files
    except Exception as e:
        try:
            from conversor.converter import convert_from_files
        except Exception:
            print("ERROR: no se pudo importar el paquete 'conversor'. Asegúrese de ejecutar desde la raíz del proyecto y que PYTHONPATH lo incluya.")
            print(e)
            sys.exit(3)

    print("Ejecutando conversor:")
    print(f" - datos: {data_path}")
    print(f" - metadata: {meta_path}")
    print(f" - config: {config_path}")
    print(f" - group_by: {group_by}")
    print(f" - out: {args.out}")

    out_csv = convert_from_files(str(data_path), str(meta_path), str(config_path), group_by, args.out)

    print("Listo. Generado:")
    print(f" - bin data: {out_csv}")


if __name__ == "__main__":
    main()
