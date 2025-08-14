import pandas as pd
import argparse
import os
from dotenv import load_dotenv
import psycopg
from psycopg import sql
from io import StringIO

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Gestor de carga de datos procesados a base de datos")
    parser.add_argument("--ruta-datos-procesados", type=str, required=True, help="Ruta de los datos procesados que serán cargados a la base de datos")
    parser.add_argument("--ruta-env", type=str, default='./.env', help="Ruta al archivo .env")
    parser.add_argument("--crear-tabla", action='store_true', help="Adicionalmente crea la tabla especificada en el archivo .env")
    args = parser.parse_args()
    
    df = pd.read_csv(args.ruta_datos_procesados)

    if not os.path.exists(args.ruta_env):
        raise FileNotFoundError(f"No se encontró el archivo .env en la ruta: {args.ruta_env}")

    load_dotenv(args.ruta_env)
    
    with psycopg.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    ) as conn:
        
        with conn.cursor() as cursor:
            
            tabla = os.getenv("DB_TABLE")
            if args.crear_tabla == True:
                tipos = {
                    'int64': 'INTEGER',
                    'float64': 'FLOAT',
                    'object': 'TEXT',
                    'bool': 'BOOLEAN'
                }
                columns = []
                for col, dtype in df.dtypes.items():
                    sql_type = tipos.get(str(dtype), 'TEXT')
                    columns.append(f"{col} {sql_type}")
                columns_sql = ",\n    ".join(columns)
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {tabla} (
                    id SERIAL PRIMARY KEY,
                    {columns_sql}
                );
                """
                cursor.execute(create_table_sql)
        
            # archivo temporal en memoria
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=True)
            buffer.seek(0)  # volver al inicio del archivo

            with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(tabla),
                sql.SQL(", ").join([sql.Identifier(col) for col in df.columns])
            )) as copy:
                copy.write(buffer.getvalue())
            
            conn.commit()
            print(f"Datos insertados exitosamente en la tabla '{tabla}'")
