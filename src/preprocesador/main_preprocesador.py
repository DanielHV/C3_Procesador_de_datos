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
        procesador_config = json.load(f)
    if 'ruta_csv_dataset' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_dataset')
    ruta_csv_dataset = procesador_config['ruta_csv_dataset']
    if not os.path.exists(ruta_csv_dataset):
        raise FileNotFoundError(f'La ruta especificada para el archivo .csv del dataset no existe ({ruta_csv_dataset})')
    df = pd.read_csv(ruta_csv_dataset)
    
    if 'ruta_csv_metadatos' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_metadatos')
    ruta_csv_metadatos = procesador_config['ruta_csv_metadatos']
    if not os.path.exists(ruta_csv_metadatos):
        raise FileNotFoundError(f'La ruta especificada para el archivo .csv de metadatos no existe ({ruta_csv_metadatos})')
    metadatos = pd.read_csv(ruta_csv_metadatos)
    metadatos.columns = [col.lower() for col in metadatos.columns]
    
    if 'columna_metadatos_nombres' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_nombres, que incluye el nombre descriptivo de cada variables')
    columna_metadatos_nombres = procesador_config['columna_metadatos_nombres'].lower()
    if not isinstance(columna_metadatos_nombres, str):
        raise TypeError('El valor asociado al campo columna_metadatos_nombres debe ser de tipo str')
    if columna_metadatos_nombres not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_nombres}')

    if 'columna_metadatos_alias' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_alias, que incluye el alias de cada variable')
    columna_metadatos_alias = procesador_config['columna_metadatos_alias']
    if not isinstance(columna_metadatos_alias, str):
        raise TypeError('El valor asociado al campo columna_metadatos_alias debe ser de tipo str')
    if columna_metadatos_alias not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_alias}')
    
    if 'columna_metadatos_posibles_valores' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_posibles_valores, que incluye cada posible valor de cada variable en formato de lista')
    columna_metadatos_posibles_valores = procesador_config['columna_metadatos_posibles_valores']
    if not isinstance(columna_metadatos_posibles_valores, str):
        raise TypeError('El valor asociado al campo columna_metadatos_posibles_valores debe ser de tipo str')
    if columna_metadatos_posibles_valores not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_posibles_valores}')
    
    if 'columna_metadatos_posibles_valores_alias' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_posibles_valores_alias, que incluye el alias de cada posible valor de cada variable en formato de lista')
    columna_metadatos_posibles_valores_alias = procesador_config['columna_metadatos_posibles_valores_alias']
    if not isinstance(columna_metadatos_posibles_valores_alias, str):
        raise TypeError('El valor asociado al campo columna_metadatos_posibles_respuestas_alias debe ser de tipo str')
    if columna_metadatos_posibles_valores_alias not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_posibles_valores_alias}')
    
    if 'columna_metadatos_tipos' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_tipos, que incluye el tipo a convertir de cada variable')
    columna_metadatos_tipos = procesador_config['columna_metadatos_tipos']
    if not isinstance(columna_metadatos_tipos, str):
        raise TypeError('El valor asociado al campo columna_metadatos_tipos debe ser de tipo str')
    if columna_metadatos_tipos not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_tipos}')
    
    if 'columna_metadatos_filtro_excluir' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_filtro_excluir, de la cual sus valores serán utilizados para filtrar variables en el dataset')
    columna_metadatos_filtro_excluir = procesador_config['columna_metadatos_filtro_excluir']
    if not isinstance(columna_metadatos_filtro_excluir, str):
        raise TypeError('El valor asociado al campo columna_metadatos_filtro_excluir debe ser de tipo str')
    if columna_metadatos_filtro_excluir not in metadatos.columns:
        raise KeyError(f'El DataFrame de metadatos no tiene una columna llamada {columna_metadatos_filtro_excluir}')
    
    if 'valores_a_excluir' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo valores_a_excluir')
    valores_a_excluir = procesador_config['valores_a_excluir']
    if not isinstance(valores_a_excluir, list):
        raise TypeError('El valor asociado al campo valores_a_excluir debe ser de tipo list')
    
    if 'ruta_salida_dataset' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_salida_dataset')
    ruta_salida_dataset = procesador_config['ruta_salida_dataset']
    
    if 'ruta_salida_metadatos' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_salida_metadatos')
    ruta_salida_metadatos = procesador_config['ruta_salida_metadatos']

    preprocesador = Preprocesador(df=df, metadatos = metadatos)
    
    if columna_metadatos_alias is not None:
        preprocesador.nombres_a_alias(columna_metadatos_nombres=columna_metadatos_nombres, 
                                      columna_metadatos_alias=columna_metadatos_alias)
        
    if columna_metadatos_tipos:
        preprocesador.convertir_tipos(
            columna_metadatos_nombres=columna_metadatos_alias, 
            columna_metadatos_tipos=columna_metadatos_tipos)
        
    if columna_metadatos_filtro_excluir:
        preprocesador.excluir_variables(
            columna_metadatos_nombres=columna_metadatos_alias, 
            columna_metadatos_filtro_excluir=columna_metadatos_filtro_excluir, 
            valores_a_excluir=valores_a_excluir
        )
        
    agrupacion = procesador_config.get('agrupacion', None)
    
    if agrupacion:
        
        if 'variables_id_agrupacion_list' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave variables_id_agrupacion_list')
        if 'variables_a_agrupar_tipos' not in agrupacion.keys():
            raise ValueError('El campo agrupacion debe tener una llave variables_a_agrupar_tipos')
        variables_id_agrupacion = agrupacion['variables_id_agrupacion_list']
        variables_a_agrupar_tipos = agrupacion['variables_a_agrupar_tipos']
        for tipo, var_dict in variables_a_agrupar_tipos.items():
            
            var_list = set(var_dict['list'])
            var_regex = var_dict['regex']
            for regex in var_regex:
                var_list = var_list | set(obtener_variables_regex_df(regex=regex, df=preprocesador.df))
            df_agregado = preprocesador.generar_datos_agregados(variables_id_agrupacion=variables_id_agrupacion,
                                                                variables_a_agrupar=var_list,
                                                                tipo_valores=tipo)
            df_agregado.to_csv(ruta_salida_dataset, index=False)
            metadatos_df_agregado = preprocesador.generar_metadatos_agregados(columna_metadatos_nombres=columna_metadatos_nombres,
                                                        columna_metadatos_alias=columna_metadatos_alias,
                                                        columna_metadatos_posibles_valores=columna_metadatos_posibles_valores,
                                                        columna_metadatos_posibles_valores_alias=columna_metadatos_posibles_valores_alias)
            metadatos_df_agregado.to_csv(ruta_salida_metadatos, index=False)
    else:
        preprocesador.df.to_csv(ruta_salida_dataset, index=False)
        preprocesador.metadatos.to_csv(ruta_salida_metadatos, index=False)