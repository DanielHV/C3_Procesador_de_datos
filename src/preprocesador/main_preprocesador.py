import os
import argparse
import json
import pandas as pd
from preprocesador import *

def obtener_variables_regex_df(df:pd.DataFrame, regex:str) -> list:
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError('El parámetro df debe ser de tipo pd.DataFrame')
    
    if not isinstance(regex, str):
        raise TypeError('El parámetro regex debe ser de tipo str')
    try:
        pattern = re.compile(regex)
    except re.error:
        raise ValueError(f'La expresión regular {regex} no es válida')
    
    return list(df.filter(regex=regex).columns)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Procesador de datos C3')
    parser.add_argument('--config', type=str, required=True, help='Archivo de configuración')
    args = parser.parse_args()
    
    with open(args.config) as f:
        preprocesador_config = json.load(f)
        
    # validacion de rutas
    
    if 'ruta_csv_dataset' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_dataset')
    ruta_csv_dataset = preprocesador_config['ruta_csv_dataset']
    if not os.path.exists(ruta_csv_dataset):
        raise FileNotFoundError(f'La ruta especificada para el archivo .csv del dataset no existe ({ruta_csv_dataset})')
    df = pd.read_csv(ruta_csv_dataset, dtype=str)
    if 'Unnamed: 0' in df.columns:
        df.drop(columns=['Unnamed: 0'], inplace=True)
    
    if 'ruta_csv_metadatos' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_metadatos')
    ruta_csv_metadatos = preprocesador_config['ruta_csv_metadatos']
    if not os.path.exists(ruta_csv_metadatos):
        raise FileNotFoundError(f'La ruta especificada para el archivo .csv de metadatos no existe ({ruta_csv_metadatos})')
    metadatos = pd.read_csv(ruta_csv_metadatos, dtype=str)
    if 'Unnamed: 0' in df.columns:
        metadatos.drop(columns=['Unnamed: 0'], inplace=True)
    
    if 'ruta_salida_dataset' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_salida_dataset')
    ruta_salida_dataset = preprocesador_config['ruta_salida_dataset']
    
    if 'ruta_salida_diccionario_traducciones' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_salida_diccionario_traducciones')
    ruta_salida_diccionario_traducciones = preprocesador_config['ruta_salida_diccionario_traducciones']
    
    # validacion de columnas indispensables (nombre y posibles valores)
    
    if 'columna_metadatos_nombres' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_nombres, que incluye el nombre descriptivo de cada variables')
    columna_metadatos_nombres = preprocesador_config['columna_metadatos_nombres']
    if not isinstance(columna_metadatos_nombres, str):
        raise TypeError('El valor asociado al campo columna_metadatos_nombres debe ser de tipo str')
    if columna_metadatos_nombres not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_nombres}')
    
    if 'columna_metadatos_posibles_valores' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_posibles_valores, que incluye cada posible valor de cada variable en formato de lista')
    columna_metadatos_posibles_valores = preprocesador_config['columna_metadatos_posibles_valores']
    if not isinstance(columna_metadatos_posibles_valores, str):
        raise TypeError('El valor asociado al campo columna_metadatos_posibles_valores debe ser de tipo str')
    if columna_metadatos_posibles_valores not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_posibles_valores}')

    # validacion de columnas opcionales para diccionario_alias

    columna_metadatos_alias = preprocesador_config.get('columna_metadatos_alias', columna_metadatos_nombres)
    if columna_metadatos_alias is not None:
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El valor asociado al campo columna_metadatos_alias debe ser de tipo str')
        if columna_metadatos_alias not in metadatos.columns:
            raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_alias}')
    
    columna_metadatos_posibles_valores_alias = preprocesador_config.get('columna_metadatos_posibles_valores_alias', columna_metadatos_posibles_valores)
    if columna_metadatos_posibles_valores_alias is not None:
        if not isinstance(columna_metadatos_posibles_valores_alias, str):
            raise TypeError('El valor asociado al campo columna_metadatos_posibles_respuestas_alias debe ser de tipo str')
        if columna_metadatos_posibles_valores_alias not in metadatos.columns:
            raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_posibles_valores_alias}')
    
    '''
    columna_metadatos_tipos = preprocesador_config.get("columna_metadatos_tipos")
    if columna_metadatos_tipos is not None:
        if not isinstance(columna_metadatos_tipos, str):
            raise TypeError('El valor asociado al campo columna_metadatos_tipos debe ser de tipo str')
        if columna_metadatos_tipos not in metadatos.columns:
            raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_tipos}')
    
    columna_metadatos_filtro_excluir = preprocesador_config.get('columna_metadatos_filtro_excluir')
    if columna_metadatos_filtro_excluir is not None:
        if not isinstance(columna_metadatos_filtro_excluir, str):
            raise TypeError('El valor asociado al campo columna_metadatos_filtro_excluir debe ser de tipo str')
        if columna_metadatos_filtro_excluir not in metadatos.columns:
            raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_filtro_excluir}')
    valores_a_excluir = preprocesador_config.get('valores_a_excluir')
    if valores_a_excluir is not None:
        if not isinstance(valores_a_excluir, list):
            raise TypeError('El valor asociado al campo valores_a_excluir debe ser de tipo list')
    '''

    # definicion de preprocesador segun datos especificados en la configuracion

    preprocesador = Preprocesador(
        df=df, 
        metadatos=metadatos,
        columna_metadatos_nombres=columna_metadatos_nombres,
        columna_metadatos_posibles_valores=columna_metadatos_posibles_valores
    )
    
    preprocesador.eliminar_cadenas_vacias()
        
    '''
    if columna_metadatos_tipos is not None:
        preprocesador.convertir_tipos(
            columna_metadatos_tipos=columna_metadatos_tipos)
    
    
    if columna_metadatos_filtro_excluir is not None:
        preprocesador.excluir_variables(
            columna_metadatos_filtro_excluir=columna_metadatos_filtro_excluir, 
            valores_a_excluir=valores_a_excluir
        )
    '''
    
    # validacion de existencia de variables identificadoras
    
    if 'variables_identificadoras_list' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variables_identificadoras_list')
    variables_identificadoras_list = preprocesador_config['variables_identificadoras_list']
    if not isinstance(variables_identificadoras_list, list):
        raise TypeError('El valor asociado al campo variables_identificadoras_list debe ser de tipo list')
    
    # validacion de existencia de variables a agrupar
    
    if 'variables_a_agrupar' not in preprocesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variables_a_agrupar')
    variables_a_agrupar = preprocesador_config['variables_a_agrupar']
    if not isinstance(variables_a_agrupar, list):
        raise TypeError('El valor asociado al campo variables_a_agrupar debe ser de tipo list')
    
    
    # agrupaciones a realizar
    
    resultados_dfs = []
    resultados_traducciones = []
    
    resultados_dfs.append(preprocesador.agrupar_total_datos(variables_id_agrupacion=variables_identificadoras_list))
    resultados_traducciones.append(preprocesador.generar_diccionario_total_datos())
    
    for agrupacion in variables_a_agrupar:
        
        # validacion de estructura de agrupacion a realizar
        
        if not isinstance(agrupacion, dict):
            raise TypeError('La lista asociada al campo preprocesamiento debe contener elementos de tipo dict')
        
        if 'tipo_variables' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave tipo_variables')
        tipo_variables = agrupacion['tipo_variables']
        print(f"Comenzando arupación de variables de tipo {tipo_variables}")
        
        if 'variables_a_agrupar_list' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave variables_a_agrupar_list')
        variables_a_agrupar_list = agrupacion['variables_a_agrupar_list']
        
        if 'variables_a_agrupar_regex' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave variables_a_agrupar_regex')
        variables_a_agrupar_regex = agrupacion['variables_a_agrupar_regex']
        
        variables_a_agrupar_clasificacion_metadatos = agrupacion.get('variables_a_agrupar_clasificacion_metadatos')
        
        # obtener variables a agrupar segun filtros
        
        variables_a_agrupar_total = set(variables_a_agrupar_list)
        
        for regex in variables_a_agrupar_regex:
            #print(f"Procesando regex {regex}")
            variables_a_agrupar_total = variables_a_agrupar_total | set(obtener_variables_regex_df(regex=regex, df=preprocesador.df))
        
        if variables_a_agrupar_clasificacion_metadatos is not None:
            columna_metadatos_filtro = variables_a_agrupar_clasificacion_metadatos['columna_metadatos_filtro']
            valores = variables_a_agrupar_clasificacion_metadatos['valores']
            
            #print(f"Se filtrarán las variables clasificadas con los valores {valores} en la columna {columna_metadatos_filtro} de los metadatos")
            metadatos_filtrados = preprocesador.metadatos.loc[preprocesador.metadatos[columna_metadatos_filtro].isin(valores)]
            variables_filtradas_clasificacion_metadatos = [var for var in metadatos_filtrados[columna_metadatos_nombres] if var in preprocesador.df.columns]    
            variables_a_agrupar_total = variables_a_agrupar_total | set(variables_filtradas_clasificacion_metadatos)
            
        variables_a_agrupar_total = sorted(variables_a_agrupar_total)
        print(f"Variables a agrupar según filtros: {variables_a_agrupar_total}")
        
        # agrupacion segun tipo de variables
        
        if tipo_variables == 'categorico':
            
            df_agregado = preprocesador.agrupar_variables_categoricas(
                variables_id_agrupacion=variables_identificadoras_list,
                variables_a_agrupar=variables_a_agrupar_total
            )

            resultados_dfs.append(df_agregado)
        
            diccionario_traducciones = preprocesador.generar_diccionario_traducciones_variables_categoricas(
                variables=variables_a_agrupar_total, 
                columna_metadatos_alias=columna_metadatos_alias, 
                columna_metadatos_posibles_valores_alias=columna_metadatos_posibles_valores_alias
            )
            
            resultados_traducciones.append(diccionario_traducciones)
            
        elif tipo_variables == 'numerico':
            
            if 'operacion' not in agrupacion.keys():
                raise ValueError('El campo agrupacion debe tener una llave operacion cuando se selecciona el valor numerico para tipo_variables')
            operacion = agrupacion['operacion']
            
            df_agregado = preprocesador.agrupar_variables_numericas(
                variables_id_agrupacion=variables_identificadoras_list,
                variables_a_agrupar=variables_a_agrupar_total,
                operacion=operacion
            )

            resultados_dfs.append(df_agregado)
            
            diccionario_traducciones = preprocesador.generar_diccionario_traducciones_variables_numericas(
                variables=variables_a_agrupar_total, 
                columna_metadatos_alias=columna_metadatos_alias, 
                operacion=operacion
            )
            
            resultados_traducciones.append(diccionario_traducciones)
            
        else:
            raise ValueError('El valor de tipo_variables debe ser una de las cadenas: categorico, numerico')
        
    # hacer join de todas las agrupaciones realizadas
    
    join_dfs = functools.reduce(
        lambda left, right: pd.merge(left, right, on=variables_identificadoras_list, how='inner'),
        resultados_dfs
    )
    
    # combinar diccionarios obtenidos por cada agrupacion

    diccionario_final = {}
    for d in resultados_traducciones:
        diccionario_final.update(d)
    diccionario_final_df = pd.DataFrame(list(diccionario_final.items()), columns=['variable', 'traduccion'])
    
    # convertir resultados a csv
    
    join_dfs.to_csv(ruta_salida_dataset, index=False)
    print(f'Preprocesamiento creado en la ruta {ruta_salida_dataset}')
    diccionario_final_df.to_csv(ruta_salida_diccionario_traducciones, index=False)
    print(f'Diccionario de traducciones creado en la ruta {ruta_salida_diccionario_traducciones}')

