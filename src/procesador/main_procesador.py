import pandas as pd
import os
import argparse
import json
from procesador.procesador import Procesador

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Procesador de datos C3')
    parser.add_argument('--config', type=str, required=True, help='Archivo de configuración')
    args = parser.parse_args()
    
    with open(args.config) as f:
        procesador_config = json.load(f)
    
    if 'rutas_csv_escalas' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo rutas_csv_escalas')
    rutas_csv_escalas = procesador_config['rutas_csv_escalas']
    
    if 'ruta_csv_diccionario_traducciones' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_diccionario_traducciones')
    ruta_csv_diccionario_traducciones = procesador_config['ruta_csv_diccionario_traducciones']
    if not os.path.exists(ruta_csv_diccionario_traducciones):
        raise FileNotFoundError('La ruta especificada para el archivo .csv de traducciones no existe')
    if 'columna_diccionario_traducciones_nombres' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_diccionario_traducciones_nombres')
    columna_diccionario_traducciones_nombres = procesador_config['columna_diccionario_traducciones_nombres']
    if 'columna_diccionario_traducciones_alias' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_diccionario_traducciones_alias')
    columna_diccionario_traducciones_alias = procesador_config['columna_diccionario_traducciones_alias']
    
    variables_excluidas_list = procesador_config.get('variables_excluidas_list', [])
    variables_excluidas_regex = procesador_config.get('variables_excluidas_regex', [])
        
    if 'variables_identificadoras' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variables_identificadoras')
    variables_identificadoras = procesador_config.get('variables_identificadoras')
    
    if 'variables_a_procesar_list' not in procesador_config and 'variables_a_procesar_regex' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener al menos uno de los campos: variables_a_procesar_list, o variables_a_procesar_regex')
    variables_a_procesar_list = procesador_config.get('variables_a_procesar_list', None)
    variables_a_procesar_regex = procesador_config.get('variables_a_procesar_regex', None)
    
    if 'q' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo q')
    q = procesador_config['q']
    
    if 'ruta_csv_salida' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_salida')
    ruta_csv_salida = procesador_config['ruta_csv_salida']
    
    dataframes_escalas = {}
    for escala, ruta in rutas_csv_escalas.items():
        if not os.path.exists(ruta):
            raise FileNotFoundError(f'La ruta especificada para el archivo .csv de la escala {escala} no existe')
        dtype_dict = {col: str for col in variables_identificadoras}
        dataframes_escalas = {escala:pd.read_csv(ruta, dtype=dtype_dict)}
        
    diccionario_traducciones = pd.read_csv(ruta_csv_diccionario_traducciones)
    if columna_diccionario_traducciones_nombres not in diccionario_traducciones.columns:
        raise ValueError(f'El DataFrame correspondiente al campo ruta_csv_diccionario_traducciones debe contener la columna {columna_diccionario_traducciones_nombres}')
    if columna_diccionario_traducciones_alias not in diccionario_traducciones.columns:
        raise ValueError(f'El DataFrame correspondiente al campo ruta_csv_diccionario_traducciones debe contener la columna {columna_diccionario_traducciones_alias}')
    
    procesador = Procesador(
        dataframes_escalas=dataframes_escalas, 
        diccionario_traducciones=diccionario_traducciones, 
        columna_diccionario_traducciones_nombres=columna_diccionario_traducciones_nombres,
        columna_diccionario_traducciones_alias=columna_diccionario_traducciones_alias,
        variables_identificadoras=variables_identificadoras,
        variables_excluidas_list=variables_excluidas_list, 
        variables_excluidas_regex=variables_excluidas_regex 
    )
    
    procesamiento_listas = pd.DataFrame()
    if variables_a_procesar_list is not None:
        if 'None' in variables_a_procesar_list:
            variables_a_procesar_list[None] = variables_a_procesar_list.pop('None')
        procesamiento_listas_dict = procesador.procesar_multiples_variables_list(escalas=list(dataframes_escalas.keys()), dicc=variables_a_procesar_list, q=q)
        procesamiento_listas = pd.concat(list(procesamiento_listas_dict.values()))
        
    procesamiento_regex = pd.DataFrame()
    if variables_a_procesar_regex is not None:
        if 'None' in variables_a_procesar_regex:
            variables_a_procesar_regex[None] = variables_a_procesar_regex.pop('None')
        procesamiento_regex_dict = procesador.procesar_multiples_variables_regex(escalas=list(dataframes_escalas.keys()), dicc=variables_a_procesar_regex, q=q)
        procesamiento_regex = pd.concat(list(procesamiento_regex_dict.values()))

    resultado = pd.DataFrame()
    if variables_a_procesar_list and variables_a_procesar_regex:
        resultado = pd.concat([procesamiento_regex, procesamiento_listas])
    elif variables_a_procesar_list:
        resultado = procesamiento_listas
    elif variables_a_procesar_regex:
        resultado = procesamiento_regex
        
    duplicados = resultado.duplicated(['code', 'bin'])

    if duplicados.any():
        print('Advertencia: el resultado del procesamiento contiene categorías duplicadas:')
        print(resultado.loc[duplicados, ['code', 'bin']])
        
    resultado.to_csv(ruta_csv_salida, index=False)
    print(f'Procesamiento finalizado, el archivo .csv resultante se encuentra en la ruta:\n{ruta_csv_salida}')