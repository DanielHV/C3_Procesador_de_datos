import os
import argparse
import json
import pandas as pd
import functools
from utils.regex_utils import obtener_variables_regex_df
from preprocesador.preprocesador import Preprocesador

if __name__ == '__main__':
    """
    Script principal para ejecutar el preprocesamiento de datos utilizando la clase Preprocesador.

    Este script toma un archivo de configuración en formato JSON que especifica las rutas de entrada,
    las variables a agrupar, y otros parámetros necesarios para el preprocesamiento. Genera un archivo
    CSV con los datos preprocesados y otro con el diccionario de alias.

    Raises:
        ValueError: Si el archivo de configuración no contiene los campos requeridos.
        FileNotFoundError: Si alguna de las rutas especificadas en el archivo de configuración no existe.
    """
    
    # definir flag de archivo de configuracion
    parser = argparse.ArgumentParser(description='Procesador de datos C3')
    parser.add_argument('--config', type=str, required=True, help='Archivo de configuración')
    args = parser.parse_args()
    
    # cargar archivo de configuracion
    with open(args.config) as f:
        preprocesador_config = json.load(f)
        
    # validaciones campo ruta_csv_dataset    
    if 'ruta_csv_dataset' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_dataset')
    ruta_csv_dataset = preprocesador_config['ruta_csv_dataset']
    if not os.path.exists(ruta_csv_dataset):
        raise FileNotFoundError(f'La ruta especificada para el archivo .csv del dataset no existe ({ruta_csv_dataset})')
    
    # cargar dataframe de datos con columnas de tipo str
    df = pd.read_csv(ruta_csv_dataset, dtype=str)
    
    # eliminar columna 'Unnamed: 0' (esta existe cuando se tiene una primera columna de indices sin nombre)
    if 'Unnamed: 0' in df.columns:
        df.drop(columns=['Unnamed: 0'], inplace=True)
    
    # validaciones campo ruta_csv_diccionario_datos    
    if 'ruta_csv_diccionario_datos' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_diccionario_datos')
    ruta_csv_diccionario_datos = preprocesador_config['ruta_csv_diccionario_datos']
    if not os.path.exists(ruta_csv_diccionario_datos):
        raise FileNotFoundError(f'La ruta especificada para el archivo .csv del diccionario de datos no existe ({ruta_csv_diccionario_datos})')
    
    # cargar dataframe de diccionario de datos con columnas de tipo str
    diccionario_datos = pd.read_csv(ruta_csv_diccionario_datos, dtype=str)
    
    # eliminar columna 'Unnamed: 0' (esta existe cuando se tiene una primera columna de indices sin nombre)
    if 'Unnamed: 0' in diccionario_datos.columns:
        diccionario_datos.drop(columns=['Unnamed: 0'], inplace=True)
    
    # validaciones campo ruta_salida_dataset
    if 'ruta_salida_dataset' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_salida_dataset')
    ruta_salida_dataset = preprocesador_config['ruta_salida_dataset']
    
    # validaciones campo ruta_salida_alias
    if 'ruta_salida_alias' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_salida_alias')
    ruta_salida_alias = preprocesador_config['ruta_salida_alias']
    
    # validaciones campo columna_diccionario_nombres
    if 'columna_diccionario_nombres' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_diccionario_nombres, que incluye el nombre descriptivo de cada variables')
    columna_diccionario_nombres = preprocesador_config['columna_diccionario_nombres']
    if not isinstance(columna_diccionario_nombres, str):
        raise TypeError('El valor asociado al campo columna_diccionario_nombres debe ser de tipo str')
    if columna_diccionario_nombres not in diccionario_datos.columns:
        raise KeyError(f'El DataFrame del diccionario de datos no tiene una columna llamada {columna_diccionario_nombres}')
    
    # validaciones campo columna_diccionario_posibles_valores
    if 'columna_diccionario_posibles_valores' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_diccionario_posibles_valores, que incluye cada posible valor de cada variable en formato de lista')
    columna_diccionario_posibles_valores = preprocesador_config['columna_diccionario_posibles_valores']
    if not isinstance(columna_diccionario_posibles_valores, str):
        raise TypeError('El valor asociado al campo columna_diccionario_posibles_valores debe ser de tipo str')
    if columna_diccionario_posibles_valores not in diccionario_datos.columns:
        raise KeyError(f'El DataFrame del diccionario de datos no tiene una columna llamada {columna_diccionario_posibles_valores}')

    # validaciones campo columna_diccionario_alias
    columna_diccionario_alias = preprocesador_config.get('columna_diccionario_alias', columna_diccionario_nombres)
    if columna_diccionario_alias is not None:
        if not isinstance(columna_diccionario_alias, str):
            raise TypeError('El valor asociado al campo columna_diccionario_alias debe ser de tipo str')
        if columna_diccionario_alias not in diccionario_datos.columns:
            raise KeyError(f'El DataFrame del diccionario de datos no tiene una columna llamada {columna_diccionario_alias}')
    
    # validaciones campo columna_diccionario_posibles_valores_alias
    columna_diccionario_posibles_valores_alias = preprocesador_config.get('columna_diccionario_posibles_valores_alias', columna_diccionario_posibles_valores)
    if columna_diccionario_posibles_valores_alias is not None:
        if not isinstance(columna_diccionario_posibles_valores_alias, str):
            raise TypeError('El valor asociado al campo columna_diccionario_posibles_respuestas_alias debe ser de tipo str')
        if columna_diccionario_posibles_valores_alias not in diccionario_datos.columns:
            raise KeyError(f'El DataFrame del diccionario de datos no tiene una columna llamada {columna_diccionario_posibles_valores_alias}')
    
    # validaciones campo columna_diccionario_tipos
    columna_diccionario_tipos = preprocesador_config.get("columna_diccionario_tipos")
    if columna_diccionario_tipos is not None:
        if not isinstance(columna_diccionario_tipos, str):
            raise TypeError('El valor asociado al campo columna_diccionario_tipos debe ser de tipo str')
        if columna_diccionario_tipos not in diccionario_datos.columns:
            raise KeyError(f'El DataFrame del diccionario de datos no tiene una columna llamada {columna_diccionario_tipos}')
    
    # validaciones campo columna_diccionario_filtro_excluir
    columna_diccionario_filtro_excluir = preprocesador_config.get('columna_diccionario_filtro_excluir')
    if columna_diccionario_filtro_excluir is not None:
        if not isinstance(columna_diccionario_filtro_excluir, str):
            raise TypeError('El valor asociado al campo columna_diccionario_filtro_excluir debe ser de tipo str')
        if columna_diccionario_filtro_excluir not in diccionario_datos.columns:
            raise KeyError(f'El DataFrame del diccionario de datos no tiene una columna llamada {columna_diccionario_filtro_excluir}')
        
    # validaciones campo valores_a_excluir
    valores_a_excluir = preprocesador_config.get('valores_a_excluir')
    if valores_a_excluir is not None:
        if not isinstance(valores_a_excluir, list):
            raise TypeError('El valor asociado al campo valores_a_excluir debe ser de tipo list')
    
    # inicializar preprocesador
    preprocesador = Preprocesador(
        df=df, 
        diccionario_datos=diccionario_datos,
        columna_diccionario_nombres=columna_diccionario_nombres,
        columna_diccionario_posibles_valores=columna_diccionario_posibles_valores
    )
    
     # eliminar de cadenas vacias en el dataframe
    preprocesador.eliminar_cadenas_vacias()
        
    # convertir tipos en el dataframe segun campos 'columna_diccionario_tipos'
    if columna_diccionario_tipos is not None:
        preprocesador.convertir_tipos(
            columna_diccionario_tipos=columna_diccionario_tipos)
    
    # excluir columnas en el dataframe segun campos 'columna_diccionario_filtro_excluir' y 'valores_a_excluir'
    if columna_diccionario_filtro_excluir is not None:
        preprocesador.excluir_variables(
            columna_diccionario_filtro_excluir=columna_diccionario_filtro_excluir, 
            valores_a_excluir=valores_a_excluir
        )

    # validaciones campo variables_identificadoras_list
    if 'variables_identificadoras_list' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variables_identificadoras_list')
    variables_identificadoras_list = preprocesador_config['variables_identificadoras_list']
    if not isinstance(variables_identificadoras_list, list):
        raise TypeError('El valor asociado al campo variables_identificadoras_list debe ser de tipo list')
    
    # validaciones campo variables_a_agrupar
    if 'variables_a_agrupar' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variables_a_agrupar')
    variables_a_agrupar = preprocesador_config['variables_a_agrupar']
    if not isinstance(variables_a_agrupar, list):
        raise TypeError('El valor asociado al campo variables_a_agrupar debe ser de tipo list')
    
    
    # inicializar de listas para almacenar resultados y diccionarios de alias
    resultados_dfs = []
    resultados_alias = []
    
    # agregar conteo total de datos y su alias a las listas de resultados correspondientes
    resultados_dfs.append(preprocesador.agrupar_total_datos(variables_id_agrupacion=variables_identificadoras_list))
    resultados_alias.append(preprocesador.generar_alias_total_datos())
    
    # obtener elementos de la lista 'variables_a_agrupar'
    for agrupacion in variables_a_agrupar:
        
        # validaciones elemento actual (agrupacion)
        if not isinstance(agrupacion, dict):
            raise TypeError('La lista asociada al campo preprocesamiento debe contener elementos de tipo dict')
        
        # validaciones subcampo tipo_variables
        if 'tipo_variables' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave tipo_variables')
        tipo_variables = agrupacion['tipo_variables']
        print(f"Comenzando arupación de variables de tipo {tipo_variables}")
        
        # validaciones subcampo variables_a_agrupar_list
        if 'variables_a_agrupar_list' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave variables_a_agrupar_list')
        variables_a_agrupar_list = agrupacion['variables_a_agrupar_list']
        
        # validaciones subcampo variables_a_agrupar_regex
        if 'variables_a_agrupar_regex' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave variables_a_agrupar_regex')
        variables_a_agrupar_regex = agrupacion['variables_a_agrupar_regex']
        
        # campo opcional variables_a_agrupar_clasificacion_diccionario
        variables_a_agrupar_clasificacion_diccionario = agrupacion.get('variables_a_agrupar_clasificacion_diccionario')
        
        # obtener variables a agrupar segun filtros variables_a_agrupar_list, variables_a_agrupar_regex y variables_a_agrupar_clasificacion_diccionario
        # primero inicializando set para almacenar variables a agrupar, primero con los elementos de variables_a_agrupar_list
        variables_a_agrupar_total = set(variables_a_agrupar_list)
        
        # evaluar regex en lista variables_a_agrupar_regex y agregar resultado a set
        for regex in variables_a_agrupar_regex:
            
            variables_a_agrupar_total = variables_a_agrupar_total | set(obtener_variables_regex_df(regex=regex, df=preprocesador.df))
            
        # obtener subcampo opcional variables_a_agrupar_clasificacion_diccionario
        if variables_a_agrupar_clasificacion_diccionario is not None:
            
            # este a su vez tiene subcampos 'columna_diccionario_filtro' y 'valores'
            columna_diccionario_filtro = variables_a_agrupar_clasificacion_diccionario['columna_diccionario_filtro']
            valores = variables_a_agrupar_clasificacion_diccionario['valores']
            
            # obtener las filas del dataframe del diccionario donde su valor para la columna 'columna_diccionario_filtro'
            # coincida con alguno de los elementos de la lista 'valores'
            diccionario_filtrados = preprocesador.diccionario_datos.loc[preprocesador.diccionario_datos[columna_diccionario_filtro].isin(valores)]
            
            # a partir de las filas del diccionario obtener las variables filtradas en la columna 'columna_diccionario_nombres'
            variables_filtradas_clasificacion_diccionario = [var for var in diccionario_filtrados[columna_diccionario_nombres] if var in preprocesador.df.columns]
            
            # agregar variables filtradas a set
            variables_a_agrupar_total = variables_a_agrupar_total | set(variables_filtradas_clasificacion_diccionario)
            
        variables_a_agrupar_total = sorted(variables_a_agrupar_total)
        
        # imprimir en terminal lista de variables a agrupar
        print(f"Variables a agrupar según filtros: {variables_a_agrupar_total}")
        
        if tipo_variables == 'categorico':
            
            # agrupacion variables categoricas
            df_agregado = preprocesador.agrupar_variables_categoricas(
                variables_id_agrupacion=variables_identificadoras_list,
                variables_a_agrupar=variables_a_agrupar_total
            )

            # agregar agrupacion a lista de resultados correspondiente
            resultados_dfs.append(df_agregado)
        
            # generar diccionario de alias para agrupacion
            diccionario_alias = preprocesador.generar_alias_variables_categoricas(
                variables=variables_a_agrupar_total, 
                columna_diccionario_alias=columna_diccionario_alias, 
                columna_diccionario_posibles_valores_alias=columna_diccionario_posibles_valores_alias
            )
            
            # agregar diccionario de alias a lista de resultados correspondiente
            resultados_alias.append(diccionario_alias)
            
        elif tipo_variables == 'numerico':
            
            # validacion subcampo operacion, solo cuando el subcampo tipo_variables es == 'numerico'
            if 'operacion' not in agrupacion.keys():
                raise ValueError('El campo agrupacion debe tener una llave operacion cuando se selecciona el valor numerico para tipo_variables')
            operacion = agrupacion['operacion']
            
            # agrupacion variables numericas
            df_agregado = preprocesador.agrupar_variables_numericas(
                variables_id_agrupacion=variables_identificadoras_list,
                variables_a_agrupar=variables_a_agrupar_total,
                operacion=operacion
            )

            # agregar agrupacion a lista de resultados correspondiente
            resultados_dfs.append(df_agregado)
            
            # generar diccionario de alias para agrupacion
            diccionario_alias = preprocesador.generar_alias_variables_numericas(
                variables=variables_a_agrupar_total, 
                columna_diccionario_alias=columna_diccionario_alias, 
                operacion=operacion
            )
            
            # agregar diccionario de alias a lista de resultados correspondiente
            resultados_alias.append(diccionario_alias)
            
        else:
            raise ValueError('El valor de tipo_variables debe ser una de las cadenas: categorico, numerico')
        
    # hacer join de todas las agrupaciones realizadas
    join_dfs = functools.reduce(
        lambda left, right: pd.merge(left, right, on=variables_identificadoras_list, how='inner'),
        resultados_dfs
    )
    
    # combinar diccionarios obtenidos por cada agrupacion
    alias_final = {}
    for d in resultados_alias:
        alias_final.update(d)
    alias_df = pd.DataFrame(list(alias_final.items()), columns=['variable', 'alias'])
    
    # guardar resultados en archivos csv
    join_dfs.to_csv(ruta_salida_dataset, index=False)
    print(f'Archivo csv de datos preprocesados creado en la ruta {ruta_salida_dataset}')
    alias_df.to_csv(ruta_salida_alias, index=False)
    print(f'Archivo csv de alias creado en la ruta {ruta_salida_alias}')

