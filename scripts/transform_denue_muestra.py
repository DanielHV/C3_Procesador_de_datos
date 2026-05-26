import base64
import csv
from collections import defaultdict

INPUT_FILE = "denue_muestra.csv"
OUTPUT_FILE = "denue_muestra_transformed.csv"


def encode_id(longitud: str, latitud: str, suffix: int = 0) -> str:
    """Encode lon/lat (and optional suffix for duplicates) into a base64 ID."""
    raw = f"{longitud}_{latitud}" if suffix == 0 else f"{longitud}_{latitud}_{suffix}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_id(row_id: str) -> dict:
    """Recover longitud, latitud (and suffix if any) from a base64 ID."""
    padding = "=" * (4 - len(row_id) % 4)
    raw = base64.urlsafe_b64decode(row_id + padding).decode()
    parts = raw.split("_")
    if len(parts) == 2:
        return {"longitud": parts[0], "latitud": parts[1]}
    return {"longitud": parts[0], "latitud": parts[1], "suffix": int(parts[2])}


def transform(input_path: str, output_path: str) -> None:
    # First pass: count coordinate occurrences and collect all var values
    coord_counts: dict[tuple, int] = defaultdict(int)
    all_vars: set[str] = set()
    with open(input_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            coord_counts[(row["longitud"], row["latitud"])] += 1
            all_vars.add(f"{row['codigo_act']}_{row['year']}")

    var_columns = sorted(all_vars)

    # Second pass: build one row per id accumulating var membership
    coord_seen: dict[tuple, int] = defaultdict(int)
    rows: dict[str, dict] = {}  # id -> {"cve_mun_resumido": str, "vars": set}

    with open(input_path, newline="", encoding="utf-8") as fin:
        for row in csv.DictReader(fin):
            coord = (row["longitud"], row["latitud"])
            if coord_counts[coord] > 1:
                coord_seen[coord] += 1
                row_id = encode_id(row["longitud"], row["latitud"], coord_seen[coord])
            else:
                row_id = encode_id(row["longitud"], row["latitud"])

            var = f"{row['codigo_act']}_{row['year']}"
            if row_id not in rows:
                rows[row_id] = {"cve_mun_resumido": row["cve_mun_resumido"], "vars": set()}
            rows[row_id]["vars"].add(var)

    # Third pass: write pivoted output (one row per id, one column per var)
    fieldnames = ["id", "cve_mun_resumido"] + var_columns
    with open(output_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        for row_id, data in rows.items():
            out_row = {"id": row_id, "cve_mun_resumido": data["cve_mun_resumido"]}
            for col in var_columns:
                out_row[col] = 1 if col in data["vars"] else 0
            writer.writerow(out_row)

    print(f"Archivo generado: {output_path}")


if __name__ == "__main__":
    transform(INPUT_FILE, OUTPUT_FILE)
