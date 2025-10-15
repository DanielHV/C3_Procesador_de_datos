import pandas as pd
import argparse
import os
from dotenv import load_dotenv
import psycopg
from psycopg import sql
from io import StringIO

if __name__ == "__main__":
    """
    Script para cargar datos procesados a una base de datos PostgreSQL, utilizando las credenciales y parámetros definidos en un archivo .env. 
    Opcionalmente, puede crear la tabla de destino si no existe, infiriendo los tipos de datos a partir del DataFrame.

    Parámetros de entrada:
        --ruta-datos-procesados: Ruta al archivo CSV con los datos procesados.
        --ruta-env: Ruta al archivo .env con las credenciales de la base de datos (por defecto: ./.env).
        --crear-tabla: Si se incluye, crea la tabla de destino automáticamente si no existe.

    Raises:
        FileNotFoundError: Si no se encuentra el archivo .env en la ruta especificada.
    """
    
    # definir flags de archivos de configuracion
    parser = argparse.ArgumentParser(description="Gestor de carga de datos procesados a base de datos")
    parser.add_argument("--ruta-datos-procesados", type=str, required=True, help="Ruta de los datos procesados que serán cargados a la base de datos")
    parser.add_argument("--ruta-env", type=str, default='./.env', help="Ruta al archivo .env")
    parser.add_argument("--crear-tabla", action='store_true', help="Adicionalmente crea la tabla especificada en el archivo .env")
    args = parser.parse_args()
    
    # cargar archivo csv de datos procesados
    df = pd.read_csv(args.ruta_datos_procesados)

    if not os.path.exists(args.ruta_env):
        raise FileNotFoundError(f"No se encontró el archivo .env en la ruta: {args.ruta_env}")

    # cargar variables de entorno desde archivo .env
    load_dotenv(args.ruta_env)
    
    # conexion postgres
    with psycopg.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    ) as conn:
        
        with conn.cursor() as cursor:
            
            tabla = os.getenv("DB_TABLE")
            
            # crear tabla si se especifica y no existe
            if args.crear_tabla == True:
                tipos = {
                    'int64': 'INTEGER',
                    'float64': 'FLOAT',
                    'object': 'TEXT',
                    'bool': 'BOOLEAN'
                }
                columns = []
                
                # inferir tipos de datos a partir de dataframe
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

            # copiar datos de buffer a tabla
            with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(tabla),
                sql.SQL(", ").join([sql.Identifier(col) for col in df.columns])
            )) as copy:
                copy.write(buffer.getvalue())
            
            conn.commit()
            print(f"Datos insertados exitosamente en la tabla '{tabla}'")
