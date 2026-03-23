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
    parser.add_argument("--ruta-datos-lugares", type=str, required=False, help="Ruta de los datos procesados de lugares (mallas) que serán cargados a la base de datos")
    parser.add_argument("--ruta-datos-personas", type=str, required=False, help="Ruta de los datos procesados del ensamble secundario que serán cargados a la base de datos")
    parser.add_argument("--tipo-ensamble", type=str, default="personas", help="Tipo de ensamble secundario (e.g. 'personas', 'establecimientos'). Define el sufijo de la tabla en la base de datos (por defecto: 'personas')")
    parser.add_argument("--ruta-datos-procesados", type=str, required=False, help="(Deprecado) Ruta al archivo CSV con los datos procesados (se asume lugares si se usa)")
    parser.add_argument("--ruta-env", type=str, default='./.env', help="Ruta al archivo .env")
    parser.add_argument("--crear-tabla", action='store_true', help="Adicionalmente crea la tabla especificada en el archivo .env")
    args = parser.parse_args()
    
    # Validar argumentos
    if not any([args.ruta_datos_lugares, args.ruta_datos_personas, args.ruta_datos_procesados]):
        raise ValueError("Se debe especificar al menos un archivo de datos: --ruta-datos-lugares, --ruta-datos-personas o --ruta-datos-procesados")

    dataframes = {}
    
    # Cargar datos de lugares
    ruta_lugares = args.ruta_datos_lugares or args.ruta_datos_procesados
    if ruta_lugares:
        if os.path.exists(ruta_lugares):
            dataframes['mun'] = pd.read_csv(ruta_lugares)
            print(f"Cargado archivo de lugares: {ruta_lugares}")
        else:
            raise FileNotFoundError(f"No se encontró el archivo de lugares: {ruta_lugares}")

    # Cargar datos del ensamble secundario
    if args.ruta_datos_personas:
        if os.path.exists(args.ruta_datos_personas):
            dataframes[args.tipo_ensamble] = pd.read_csv(args.ruta_datos_personas)
            print(f"Cargado archivo de ensamble '{args.tipo_ensamble}': {args.ruta_datos_personas}")
        else:
            raise FileNotFoundError(f"No se encontró el archivo de ensamble '{args.tipo_ensamble}': {args.ruta_datos_personas}")

    if not os.path.exists(args.ruta_env):
        raise FileNotFoundError(f"No se encontró el archivo .env en la ruta: {args.ruta_env}")

    # cargar variables de entorno desde archivo .env
    load_dotenv(args.ruta_env)
    
    # Procesar columnas especiales y diccionarios independientes
    df_dicts = {}
    dict_cols_per_df = {}
    
    # Tipos especiales por dataframe
    special_types_per_df = {} # {key: {col: type}}

    for key, df in dataframes.items():
        special_types = {}
        for col in df.columns:
            if col.startswith("cells_"):
                # verificar contenido para decidir entre INTEGER[] o TEXT[]
                sample = df[col].dropna()
                is_integer_array = False
                if not sample.empty:
                    # Tomar el primer valor no nulo
                    val = str(sample.iloc[0])
                    # Limpiar llaves
                    val_content = val.replace('{', '').replace('}', '').strip()
                    if val_content:
                        # Verificar el primer elemento
                        first_elem = val_content.split(',')[0].strip()
                        # Si es digito, asumimos entero. Si tiene comillas o caracteres alfa, texto.
                        # Ojo: ID_REGISTRO es alfanumerico (e.g. 109fd5), asi que isdigit() sera False.
                        if first_elem.isdigit():
                            is_integer_array = True
                
                special_types[col] = "INTEGER[]" if is_integer_array else "TEXT[]"
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
        
        special_types_per_df[key] = special_types

        # identificar columnas de diccionario y valores
        cells_cols = [c for c in df.columns if c.startswith('cells_')]
        data_cols = ['bin'] + [c for c in df.columns if c.startswith('interval_')] + cells_cols
        dict_cols = [c for c in df.columns if c not in data_cols]
        dict_cols_per_df[key] = dict_cols
        
        # Extraer filas para el diccionario
        df_dict_unique = df[dict_cols].drop_duplicates().reset_index(drop=True)
        df_dict_unique['valor'] = df_dict_unique['name'].apply(lambda x: x.split('-', 1)[1].strip() if '-' in str(x) else str(x).strip())
        df_dict_unique['variable_name'] = df_dict_unique['name'].apply(lambda x: x.split('-', 1)[0].strip() if '-' in str(x) else str(x).strip())
        df_dict_unique['description'] = df_dict_unique['descripcion']        
        
        # 1. Tabla dict (variables)
        df_vars = df_dict_unique[['variable_name', 'descripcion']].drop_duplicates(subset=['variable_name']).reset_index(drop=True)
        df_vars = df_vars.rename(columns={'descripcion': 'description'})
        df_vars['id'] = df_vars.index + 1

        print(df_vars)
        
        # Merge df_vars into the df to get dict_id
        df['variable_name'] = df['name'].apply(lambda x: x.split('-', 1)[0].strip() if '-' in str(x) else str(x).strip())
        df['valor'] = df['name'].apply(lambda x: x.split('-', 1)[1].strip() if '-' in str(x) else str(x).strip())
        df = df.merge(df_vars, on='variable_name', how='left').rename(columns={'id': 'dict_id'})
        
        # 2. Tabla values
        interval_cols = [c for c in df.columns if c.startswith('interval_')]
        val_cols_to_extract = ['dict_id', 'valor', 'code', 'bin'] + interval_cols
        df_vals = df[val_cols_to_extract].copy()
        df_vals = df_vals.rename(columns={'code': 'alias'})
        df_vals['alias'] = df_vals['alias'].apply(lambda x: str(x).split('-', 1)[1].strip() if '-' in str(x) else str(x).strip())
        df_vals['id'] = df_vals.index + 1
        
        # columns for generating table
        val_cols_for_table = ['id', 'dict_id', 'valor', 'alias', 'bin'] + interval_cols
        
        df_dicts[key] = {
            'vars': df_vars,
            'vals': df_vals[val_cols_for_table]
        }
        
        # Merge values_id de vuelta a cada dataframe
        df['values_id'] = df_vals['id']
        dataframes[key] = df

    # conexion postgres
    with psycopg.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    ) as conn:
        
        with conn.cursor() as cursor:
            
            tabla_base = os.getenv("DB_TABLE")
            
            # Tipos de datos básicos
            tipos = {
                'int64': 'INTEGER',
                'float64': 'FLOAT',
                'object': 'TEXT',
                'bool': 'BOOLEAN'
            }

            for key, df in dataframes.items():
                suffix = f'_{key}'
                tabla_destino = f"{tabla_base}{suffix}"
                tabla_dict = f"dict_{tabla_base}{suffix}"
                
                df_vars = df_dicts[key]['vars']
                df_vals = df_dicts[key]['vals']
                dict_cols = dict_cols_per_df[key]
                tabla_vals = f"values_{tabla_base}{suffix}"

                # --- 1. Crear e insertar en tabla diccionario (variables) ---
                if args.crear_tabla:
                    create_dict_sql = f"""
                    CREATE TABLE IF NOT EXISTS {tabla_dict} (
                        id INTEGER PRIMARY KEY,
                        variable_name TEXT,
                        description TEXT
                    );
                    """
                    cursor.execute(create_dict_sql)
                    cursor.execute(f"TRUNCATE TABLE {tabla_dict} CASCADE;")

                # Insertar datos vars
                buffer_vars = StringIO()
                df_vars[['id', 'variable_name', 'description']].to_csv(buffer_vars, index=False, header=True)
                buffer_vars.seek(0)
                
                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(tabla_dict),
                    sql.SQL("id, variable_name, description")
                )) as copy:
                    copy.write(buffer_vars.getvalue())
                
                print(f"Datos insertados exitosamente en la tabla diccionario '{tabla_dict}'")

                # --- 2. Crear e insertar en tabla values ---
                if args.crear_tabla:
                    val_columns_sql = [
                        "id INTEGER PRIMARY KEY",
                        f"dict_id INTEGER REFERENCES {tabla_dict}(id)",
                        "valor TEXT",
                        "alias TEXT",
                        "bin INTEGER"
                    ]
                    
                    interval_cols = [c for c in df_vals.columns if c.startswith('interval_')]
                    for col in interval_cols:
                        if col in special_types_per_df[key]:
                            sql_type = special_types_per_df[key][col]
                        else:
                            dtype = df_vals[col].dtype
                            sql_type = tipos.get(str(dtype), 'TEXT')
                        val_columns_sql.append(f"{col} {sql_type}")
                        
                    create_vals_sql = f"""
                    CREATE TABLE IF NOT EXISTS {tabla_vals} (
                        {", ".join(val_columns_sql)}
                    );
                    """
                    cursor.execute(f"DROP TABLE IF EXISTS {tabla_vals} CASCADE")
                    cursor.execute(create_vals_sql)

                # Insertar datos vals
                buffer_vals = StringIO()
                val_cols_for_table = ['id', 'dict_id', 'valor', 'alias', 'bin'] + [c for c in df_vals.columns if c.startswith('interval_')]
                df_vals[val_cols_for_table].to_csv(buffer_vals, index=False, header=True)
                buffer_vals.seek(0)
                
                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(tabla_vals),
                    sql.SQL(", ").join([sql.Identifier(col) for col in val_cols_for_table])
                )) as copy:
                    copy.write(buffer_vals.getvalue())
                
                print(f"Datos insertados exitosamente en la tabla de valores '{tabla_vals}'")

                # --- 3. Crear e insertar en tablas de datos principal ---
                cols_to_exclude = set(dict_cols + ['values_id', 'bin', 'variable_name', 'valor', 'dict_id', 'description', 'descripcion'] + [c for c in df.columns if c.startswith('interval_')])
                data_cols_current = [c for c in df.columns if c not in cols_to_exclude and c != 'id']
                
                if args.crear_tabla:
                    main_columns_sql = []
                    main_columns_sql.append(f"values_id INTEGER REFERENCES {tabla_vals}(id)")
                    
                    for col in data_cols_current:
                        if col in special_types_per_df[key]:
                            sql_type = special_types_per_df[key][col]
                        else:
                            dtype = df[col].dtype
                            sql_type = tipos.get(str(dtype), 'TEXT')
                        main_columns_sql.append(f"{col} {sql_type}")
                    
                    create_main_sql = f"""
                    CREATE TABLE IF NOT EXISTS {tabla_destino} (
                        id SERIAL PRIMARY KEY,
                        {", ".join(main_columns_sql)}
                    );
                    """
                    cursor.execute(f"DROP TABLE IF EXISTS {tabla_destino} CASCADE")
                    cursor.execute(create_main_sql)

                # Insertar datos main
                buffer_main = StringIO()
                cols_main_insert = ['values_id'] + data_cols_current
                df[cols_main_insert].to_csv(buffer_main, index=False, header=True)
                buffer_main.seek(0)

                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(tabla_destino),
                    sql.SQL(", ").join([sql.Identifier(col) for col in cols_main_insert])
                )) as copy:
                    copy.write(buffer_main.getvalue())
                
                print(f"Datos insertados exitosamente en la tabla '{tabla_destino}'")
            
            conn.commit()
