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
    
    # procesamiento de columnas especiales y determinacion de tipos
    special_types = {}
    for col in df.columns:
        if col.startswith("cells_"):
            special_types[col] = "INTEGER[]"
        elif col.startswith("interval_"):
            # verificar si tiene formato de rango con ':'
            sample = df[col].dropna()
            if not sample.empty and ':' in str(sample.iloc[0]):
                special_types[col] = "NUMRANGE"
                # transformar formato "min:max" a "[min,max)"
                def transform_range(val):
                    if pd.isna(val): return val
                    s = str(val).replace('%', '').strip()
                    if ':' in s:
                        parts = s.split(':')
                        # manejar posibles espacios o vacios
                        return f"[{parts[0].strip()},{parts[1].strip()})"
                    return val
                df[col] = df[col].apply(transform_range)
            else:
                special_types[col] = "TEXT"

    # identificar columnas de diccionario (excluyendo bin, interval_*, cells_*)
    data_cols = ['bin'] + [c for c in df.columns if c.startswith('interval_') or c.startswith('cells_')]
    dict_cols = [c for c in df.columns if c not in data_cols]
    
    # crear dataframe de diccionario unico
    df_dict = df[dict_cols].drop_duplicates().reset_index(drop=True)
    # crear dict_id (postgres ids suelen ser 1-based)
    df_dict['id'] = df_dict.index + 1
    
    # merge dict_id de vuelta al dataframe principal
    df = df.merge(df_dict, on=dict_cols, how='left')
    df = df.rename(columns={'id': 'dict_id'})
    
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
            tabla_dict = f"dict_{tabla}"
            
            # crear tablas si se especifica y no existen
            if args.crear_tabla == True:
                tipos = {
                    'int64': 'INTEGER',
                    'float64': 'FLOAT',
                    'object': 'TEXT',
                    'bool': 'BOOLEAN'
                }
                
                # 1. Crear tabla diccionario
                dict_columns_sql = []
                for col in dict_cols:
                    dtype = df_dict[col].dtype
                    sql_type = tipos.get(str(dtype), 'TEXT')
                    dict_columns_sql.append(f"{col} {sql_type}")
                
                create_dict_sql = f"""
                CREATE TABLE IF NOT EXISTS {tabla_dict} (
                    id INTEGER PRIMARY KEY,
                    {", ".join(dict_columns_sql)}
                );
                """
                cursor.execute(create_dict_sql)
                
                # 2. Crear tabla principal
                main_columns_sql = []
                # primero dict_id foreign key
                main_columns_sql.append(f"dict_id INTEGER REFERENCES {tabla_dict}(id)")
                # luego columnas de datos
                for col in data_cols:
                    if col in special_types:
                        sql_type = special_types[col]
                    else:
                        dtype = df[col].dtype
                        sql_type = tipos.get(str(dtype), 'TEXT')
                    main_columns_sql.append(f"{col} {sql_type}")
                
                create_main_sql = f"""
                CREATE TABLE IF NOT EXISTS {tabla} (
                    id SERIAL PRIMARY KEY,
                    {", ".join(main_columns_sql)}
                );
                """
                cursor.execute(create_main_sql)
        
            # insertar datos en tabla diccionario
            buffer_dict = StringIO()
            # asegurar orden de columnas: id, [dict_cols]
            cols_dict_ordered = ['id'] + dict_cols
            df_dict[cols_dict_ordered].to_csv(buffer_dict, index=False, header=True)
            buffer_dict.seek(0)
            
            with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(tabla_dict),
                sql.SQL(", ").join([sql.Identifier(col) for col in cols_dict_ordered])
            )) as copy:
                copy.write(buffer_dict.getvalue())
                
            print(f"Datos insertados exitosamente en la tabla '{tabla_dict}'")

            # insertar datos en tabla principal
            buffer_main = StringIO()
            # asegurar orden de columnas: dict_id, [data_cols]
            # nota: el dataframe original 'df' ahora tiene 'dict_id' y columnas originales.
            # seleccionamos las columnas a insertar. 'id' (PK) es SERIAL, no lo insertamos.
            cols_main_ordered = ['dict_id'] + data_cols
            df[cols_main_ordered].to_csv(buffer_main, index=False, header=True)
            buffer_main.seek(0)

            with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(tabla),
                sql.SQL(", ").join([sql.Identifier(col) for col in cols_main_ordered])
            )) as copy:
                copy.write(buffer_main.getvalue())
            
            conn.commit()
            print(f"Datos insertados exitosamente en la tabla '{tabla}'")
