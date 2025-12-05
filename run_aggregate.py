#!/usr/bin/env python3
"""Runner script to execute the agrupador aggregate_from_files with a simple CLI.

Usage example (from project root):
  python run_aggregate.py \
    --data "COVID19MEXICO_rework_v2.csv" \
    --meta "240708 Descriptores_rework_v2.csv" \
    --config mi_config.json \
    --group-by ENTIDAD_RES \
    --out out_aggregated.csv \
    --out-meta out_metadata.csv

Defaults: config=mi_config.json, group_by=ENTIDAD_RES, out files as above.
"""
import argparse
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Ejecutar módulo agrupador en datos + metadatsos a partir de un archivo de configuración JSON")
    p.add_argument("--data", required=True, help="Ruta al archivo de datos CSV")
    p.add_argument("--meta", required=True, help="Ruta al archivo de metadatos CSV")
    p.add_argument("--config", required=True, help="Ruta al archivo de configuración JSON")
    p.add_argument("--group-by", required=True, help="Columnas usadas para agrupar los datos (separar con comas)")
    p.add_argument("--out", default="out_aggregated.csv", help="Ruta de salida de datos agrupados en archivo CSV")
    p.add_argument("--out-meta", default="out_metadata.csv", help="Ruta de salida de metadatos agrupados en archivo CSV")
    p.add_argument("--ignore-conversion-errors", action="store_true",
                   help="Ignorar errores de conversión numérica (warn y continuar) en lugar de abortar")
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
        from agrupador import aggregate_from_files
    except Exception as e:
        print("ERROR: no se pudo importar el paquete 'agrupador'. Asegúrese de ejecutar desde la raíz del proyecto y que PYTHONPATH lo incluya.")
        print(e)
        sys.exit(3)

    print("Ejecutando agregación:")
    print(f" - datos: {data_path}")
    print(f" - metadata: {meta_path}")
    print(f" - config: {config_path}")
    print(f" - group_by: {group_by}")
    print(f" - out: {args.out}")
    print(f" - out_meta: {args.out_meta}")

    out_csv, out_meta = aggregate_from_files(
        str(data_path), str(meta_path), str(config_path), group_by, args.out, args.out_meta,
        ignore_conversion_errors=args.ignore_conversion_errors,
    )

    print("Listo. Generado:")
    print(f" - datos agregados: {out_csv}")
    print(f" - metadata: {out_meta}")


if __name__ == "__main__":
    main()
