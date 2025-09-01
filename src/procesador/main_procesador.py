from procesador import *
import argparse
import json
import re

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Procesador de datos C3')
    parser.add_argument('--config', type=str, required=True, help='Archivo de configuración')
    args = parser.parse_args()
    
    with open(args.config) as f:
        procesador_config = json.load(f)
    
    if 'rutas_csv_escalas' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo rutas_csv_escalas')
    rutas_csv_escalas = procesador_config['rutas_csv_escalas']
    dataframes_escalas = {}
    for escala, ruta in rutas_csv_escalas.items():
        if not os.path.exists(ruta):
            raise FileNotFoundError(f'La ruta especificada para el archivo .csv de la escala {escala} no existe')
        dataframes_escalas = {escala:pd.read_csv(ruta)}
    
    if 'ruta_csv_metadatos' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_metadatos')
    ruta_csv_metadatos = procesador_config['ruta_csv_metadatos']
    if not os.path.exists(ruta_csv_metadatos):
        raise FileNotFoundError('La ruta especificada para el archivo .csv de metadatos no existe')
    if 'columna_metadatos_nombres' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_nombres')
    columna_metadatos_nombres = procesador_config['columna_metadatos_nombres']
    if 'columna_metadatos_alias' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_metadatos_alias')
    columna_metadatos_alias = procesador_config['columna_metadatos_alias']
    metadatos = pd.read_csv(ruta_csv_metadatos)
    metadatos.columns = [col.lower() for col in metadatos.columns]
    if columna_metadatos_nombres not in metadatos.columns:
        raise ValueError(f'El DataFrame correspondiente al campo ruta_csv_metadatos debe contener la columna {columna_metadatos_nombres}')
    if columna_metadatos_alias not in metadatos.columns:
        raise ValueError(f'El DataFrame correspondiente al campo ruta_csv_metadatos debe contener la columna {columna_metadatos_alias}')
    
    variables_excluidas_list = procesador_config.get('variables_excluidas_list', [])
    variables_excluidas_regex = procesador_config.get('variables_excluidas_regex', [])
        
    if 'variable_identificadora' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variable_identificadora')
    variable_identificadora = procesador_config.get('variable_identificadora')
    
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
    
    procesador = Procesador(
        dataframes_escalas=dataframes_escalas, 
        metadatos=metadatos, 
        columna_metadatos_nombres=columna_metadatos_nombres,
        columna_metadatos_alias=columna_metadatos_alias,
        variable_identificadora=variable_identificadora,
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