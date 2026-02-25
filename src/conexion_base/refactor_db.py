import sys

with open('/home/pedro/Dev/c3/C3_Procesador_de_datos/src/conexion_base/conexion_base_datos.py', 'r') as f:
    content = f.read()

target1 = """    # Procesar columnas especiales y unificar diccionario
    all_dict_rows = []
    
    # Tipos especiales por dataframe"""

repl1 = """    # Procesar columnas especiales y diccionarios independientes
    df_dicts = {}
    dict_cols_per_df = {}
    
    # Tipos especiales por dataframe"""

content = content.replace(target1, repl1)

target2 = """        # identificar columnas de diccionario (excluyendo bin, interval_*, cells_*)
        # Asumimos que las columnas de diccionario son comunes o al menos no conflictivas
        data_cols = ['bin'] + [c for c in df.columns if c.startswith('interval_') or c.startswith('cells_')]
        dict_cols = [c for c in df.columns if c not in data_cols]
        
        # Extraer filas para el diccionario maestro
        all_dict_rows.append(df[dict_cols])

    # Crear diccionario maestro unificado
    if all_dict_rows:
        df_dict_master = pd.concat(all_dict_rows).drop_duplicates().reset_index(drop=True)
        df_dict_master['id'] = df_dict_master.index + 1
        
        # Identificar columnas del diccionario (usando el primero como referencia, deberían ser consistentes)
        # Ojo: si hay columnas disjuntas entre archivos, esto podría necesitar ajuste. 
        # Asumimos que "lugares" y "personas" comparten la estructura de metavariables.
        dict_cols_master = [c for c in df_dict_master.columns if c != 'id']
    else:
        raise ValueError("No se pudieron extraer datos para el diccionario.")

    # Merge dict_id de vuelta a cada dataframe
    for key in dataframes:
        dataframes[key] = dataframes[key].merge(df_dict_master, on=dict_cols_master, how='left')
        dataframes[key] = dataframes[key].rename(columns={'id': 'dict_id'})"""

repl2 = """        # identificar columnas de diccionario
        data_cols = ['bin'] + [c for c in df.columns if c.startswith('interval_') or c.startswith('cells_')]
        dict_cols = [c for c in df.columns if c not in data_cols]
        dict_cols_per_df[key] = dict_cols
        
        # Extraer filas para el diccionario de este dataframe
        df_dict = df[dict_cols].drop_duplicates().reset_index(drop=True)
        df_dict['id'] = df_dict.index + 1
        df_dicts[key] = df_dict
        
        # Merge dict_id de vuelta a cada dataframe
        dataframes[key] = df.merge(df_dict, on=dict_cols, how='left')
        dataframes[key] = dataframes[key].rename(columns={'id': 'dict_id'})"""

content = content.replace(target2, repl2)

target3 = """            tabla_base = os.getenv("DB_TABLE")
            tabla_dict = f"dict_{tabla_base}"
            
            # Tipos de datos básicos
            tipos = {
                'int64': 'INTEGER',
                'float64': 'FLOAT',
                'object': 'TEXT',
                'bool': 'BOOLEAN'
            }

            # --- 1. Crear e insertar en tabla diccionario (compartida) ---
            if args.crear_tabla:
                dict_columns_sql = []
                for col in dict_cols_master:
                    dtype = df_dict_master[col].dtype
                    sql_type = tipos.get(str(dtype), 'TEXT')
                    dict_columns_sql.append(f"{col} {sql_type}")
                
                create_dict_sql = f\"\"\"
                CREATE TABLE IF NOT EXISTS {tabla_dict} (
                    id INTEGER PRIMARY KEY,
                    {", ".join(dict_columns_sql)}
                );
                \"\"\"
                cursor.execute(create_dict_sql)

            # Insertar datos en tabla diccionario
            # Verificar si ta tabla esta vacia o manejar duplicados es complejo, 
            # aqui asumimos una carga limpia o que "id" es consistente.
            # Para simplificar y evitar conflictos de PK si ya existen datos, 
            # idealmente deberiamos hacer UPSERT o truncate. 
            # Por ahora, usamos TRUNCATE si se pide crear tabla, o asumimos append.
            # PERO: si la tabla ya tiene datos, los IDs podrian chocar si no se sincronizan.
            # En este script simple, si se crea tabla, se inserta todo.
            if args.crear_tabla:
                 cursor.execute(f"TRUNCATE TABLE {tabla_dict} CASCADE;")

            buffer_dict = StringIO()
            cols_dict_ordered = ['id'] + dict_cols_master
            df_dict_master[cols_dict_ordered].to_csv(buffer_dict, index=False, header=True)
            buffer_dict.seek(0)
            
            with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(tabla_dict),
                sql.SQL(", ").join([sql.Identifier(col) for col in cols_dict_ordered])
            )) as copy:
                copy.write(buffer_dict.getvalue())
            
            print(f"Datos insertados exitosamente en la tabla diccionario '{tabla_dict}'")

            # --- 2. Crear e insertar en tablas de datos (lugares/personas) ---
            for key, df in dataframes.items():
                if key == 'mun':
                    suffix = '_mun'
                elif key == 'personas':
                    suffix = '_personas'
                else:
                    suffix = '' # Fallback
                
                tabla_destino = f"{tabla_base}{suffix}"
                
                # Columnas de datos para esta tabla
                # Excluir 'bin' porque es la variable independiente clave
                # Identificar columnas que no son dict_id ni del diccionario original
                cols_to_exclude = set(dict_cols_master + ['dict_id'])
                # Pero necesitamos 'dict_id' y 'bin' y las de valores
                
                data_cols_current = [c for c in df.columns if c not in cols_to_exclude and c != 'id'] # 'id' si viniera del merge
                # Asegurar que 'dict_id' este presente y sea la FK
                
                if args.crear_tabla:
                    main_columns_sql = []
                    main_columns_sql.append(f"dict_id INTEGER REFERENCES {tabla_dict}(id)")
                    
                    for col in data_cols_current:
                        if col in special_types_per_df[key]:
                            sql_type = special_types_per_df[key][col]
                        else:
                            dtype = df[col].dtype
                            sql_type = tipos.get(str(dtype), 'TEXT')
                        main_columns_sql.append(f"{col} {sql_type}")
                    
                    create_main_sql = f\"\"\"
                    CREATE TABLE IF NOT EXISTS {tabla_destino} (
                        id SERIAL PRIMARY KEY,
                        {", ".join(main_columns_sql)}
                    );
                    \"\"\"
                    cursor.execute(f"DROP TABLE IF EXISTS {tabla_destino}") # Reiniciar si se crea de nuevo para evitar conflictos de esquema
                    cursor.execute(create_main_sql)

                # Insertar datos
                buffer_main = StringIO()
                cols_main_insert = ['dict_id'] + data_cols_current
                df[cols_main_insert].to_csv(buffer_main, index=False, header=True)
                buffer_main.seek(0)

                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(tabla_destino),
                    sql.SQL(", ").join([sql.Identifier(col) for col in cols_main_insert])
                )) as copy:
                    copy.write(buffer_main.getvalue())
                
                print(f"Datos insertados exitosamente en la tabla '{tabla_destino}'")"""

repl3 = """            tabla_base = os.getenv("DB_TABLE")
            
            # Tipos de datos básicos
            tipos = {
                'int64': 'INTEGER',
                'float64': 'FLOAT',
                'object': 'TEXT',
                'bool': 'BOOLEAN'
            }

            for key, df in dataframes.items():
                if key == 'mun':
                    suffix = '_mun'
                elif key == 'personas':
                    suffix = '_personas'
                else:
                    suffix = '' # Fallback
                
                tabla_destino = f"{tabla_base}{suffix}"
                tabla_dict = f"dict_{tabla_base}{suffix}"
                
                df_dict = df_dicts[key]
                dict_cols = dict_cols_per_df[key]

                # --- 1. Crear e insertar en tabla diccionario ---
                if args.crear_tabla:
                    dict_columns_sql = []
                    for col in dict_cols:
                        dtype = df_dict[col].dtype
                        sql_type = tipos.get(str(dtype), 'TEXT')
                        dict_columns_sql.append(f"{col} {sql_type}")
                    
                    create_dict_sql = f\"\"\"
                    CREATE TABLE IF NOT EXISTS {tabla_dict} (
                        id INTEGER PRIMARY KEY,
                        {", ".join(dict_columns_sql)}
                    );
                    \"\"\"
                    cursor.execute(create_dict_sql)
                    cursor.execute(f"TRUNCATE TABLE {tabla_dict} CASCADE;")

                # Insertar datos diccionario
                buffer_dict = StringIO()
                cols_dict_ordered = ['id'] + dict_cols
                df_dict[cols_dict_ordered].to_csv(buffer_dict, index=False, header=True)
                buffer_dict.seek(0)
                
                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(tabla_dict),
                    sql.SQL(", ").join([sql.Identifier(col) for col in cols_dict_ordered])
                )) as copy:
                    copy.write(buffer_dict.getvalue())
                
                print(f"Datos insertados exitosamente en la tabla diccionario '{tabla_dict}'")

                # --- 2. Crear e insertar en tablas de datos ---
                cols_to_exclude = set(dict_cols + ['dict_id'])
                data_cols_current = [c for c in df.columns if c not in cols_to_exclude and c != 'id']
                
                if args.crear_tabla:
                    main_columns_sql = []
                    main_columns_sql.append(f"dict_id INTEGER REFERENCES {tabla_dict}(id)")
                    
                    for col in data_cols_current:
                        if col in special_types_per_df[key]:
                            sql_type = special_types_per_df[key][col]
                        else:
                            dtype = df[col].dtype
                            sql_type = tipos.get(str(dtype), 'TEXT')
                        main_columns_sql.append(f"{col} {sql_type}")
                    
                    create_main_sql = f\"\"\"
                    CREATE TABLE IF NOT EXISTS {tabla_destino} (
                        id SERIAL PRIMARY KEY,
                        {", ".join(main_columns_sql)}
                    );
                    \"\"\"
                    cursor.execute(f"DROP TABLE IF EXISTS {tabla_destino}")
                    cursor.execute(create_main_sql)

                # Insertar datos main
                buffer_main = StringIO()
                cols_main_insert = ['dict_id'] + data_cols_current
                df[cols_main_insert].to_csv(buffer_main, index=False, header=True)
                buffer_main.seek(0)

                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(tabla_destino),
                    sql.SQL(", ").join([sql.Identifier(col) for col in cols_main_insert])
                )) as copy:
                    copy.write(buffer_main.getvalue())
                
                print(f"Datos insertados exitosamente en la tabla '{tabla_destino}'")"""

content = content.replace(target3, repl3)

with open('/home/pedro/Dev/c3/C3_Procesador_de_datos/src/conexion_base/conexion_base_datos.py', 'w') as f:
    f.write(content)
print("done")
