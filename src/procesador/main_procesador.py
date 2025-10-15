import pandas as pd
import os
import argparse
import json
import time
from procesador.procesador import Procesador

if __name__ == '__main__':
    """
    Script principal para ejecutar el procesamiento de datos utilizando la clase Procesador.
    
    Este script toma un archivo de configuración en formato JSON que especifica las rutas de entrada, 
    las variables a procesar, y otros parámetros necesarios para el procesamiento. Genera un archivo 
    CSV con los resultados del procesamiento.
    
    Raises:
        ValueError: Si el archivo de configuración no contiene los campos requeridos.
        FileNotFoundError: Si alguna de las rutas especificadas en el archivo de configuración no existe.
    """
    
    # timestamp de inicio de ejecucion del programa
    timestamp_inicio = time.time()
    
    # definir flag de archivo de configuracion
    parser = argparse.ArgumentParser(description='Procesador de datos C3')
    parser.add_argument('--config', type=str, required=True, help='Archivo de configuración')
    args = parser.parse_args()
    
    # cargar archivo de configuracion
    with open(args.config) as f:
        procesador_config = json.load(f)
    
    # validaciones campo rutas_csv_escalas
    if 'rutas_csv_escalas' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo rutas_csv_escalas')
    rutas_csv_escalas = procesador_config['rutas_csv_escalas']
    
    # validaciones campo ruta_csv_diccionario_traducciones
    if 'ruta_csv_diccionario_traducciones' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_diccionario_traducciones')
    ruta_csv_diccionario_traducciones = procesador_config['ruta_csv_diccionario_traducciones']
    if not os.path.exists(ruta_csv_diccionario_traducciones):
        raise FileNotFoundError('La ruta especificada para el archivo .csv de traducciones no existe')
    
    # validaciones campo columna_diccionario_traducciones_nombres
    if 'columna_diccionario_traducciones_nombres' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_diccionario_traducciones_nombres')
    columna_diccionario_traducciones_nombres = procesador_config['columna_diccionario_traducciones_nombres']
    
    # validaciones campo columna_diccionario_traducciones_alias
    if 'columna_diccionario_traducciones_alias' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo columna_diccionario_traducciones_alias')
    columna_diccionario_traducciones_alias = procesador_config['columna_diccionario_traducciones_alias']
    
    # obtener listas de variables a excluir mediante lista explicita y/o lista de expresiones regulares,
    # en caso de existir el campo en el archivo de configuracion
    variables_excluidas_list = procesador_config.get('variables_excluidas_list', [])
    variables_excluidas_regex = procesador_config.get('variables_excluidas_regex', [])
    
    # validaciones campo variables_identificadoras
    if 'variables_identificadoras' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo variables_identificadoras')
    variables_identificadoras = procesador_config.get('variables_identificadoras')
    if not isinstance(variables_identificadoras, dict):
        raise ValueError('El campo variables_identificadoras debe ser un diccionario {escala: [col1, col2, ...]}')
    
    # validaciones campos variables_a_procesar_list y variables_a_procesar_regex
    if 'variables_a_procesar_list' not in procesador_config and 'variables_a_procesar_regex' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener al menos uno de los campos: variables_a_procesar_list, o variables_a_procesar_regex')
    variables_a_procesar_list = procesador_config.get('variables_a_procesar_list', None)
    variables_a_procesar_regex = procesador_config.get('variables_a_procesar_regex', None)
    
    # validaciones campo q
    if 'q' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo q')
    q = procesador_config['q']
    
    # validaciones campo ruta_csv_salida
    if 'ruta_csv_salida' not in procesador_config:
        raise ValueError('El archivo JSON pasado para --config debe tener el campo ruta_csv_salida')
    ruta_csv_salida = procesador_config['ruta_csv_salida']
    
    # inicializar diccionario para almacenar dataframes de escalas especificadas
    dataframes_escalas = {}
    
    for escala, ruta in rutas_csv_escalas.items():
        
        # validaciones valores diccionario rutas_csv_escalas (rutas de archivos csv de dataframes)
        if not os.path.exists(ruta):
            raise FileNotFoundError(f'La ruta especificada para el archivo .csv de la escala {escala} no existe')
        
        # validaciones diccionario variables identificadoras
        if escala not in variables_identificadoras:
            raise ValueError(f'El diccionario de variables identificadoras no contiene la escala {escala}')
        
        # obtener variables identificadoras para escala
        id_cols = variables_identificadoras[escala]
        
        # cargar dataframe de escala actual con columnas identificadoras de tipo str
        dtype_dict = {col: str for col in id_cols}
        dataframes_escalas[escala] = pd.read_csv(ruta, dtype=dtype_dict)
        
    # cargar dataframe de diccionario de traducciones
    diccionario_traducciones = pd.read_csv(ruta_csv_diccionario_traducciones)
    
    # validaciones columna_diccionario_traducciones_nombres y columna_diccionario_traducciones_alias
    if columna_diccionario_traducciones_nombres not in diccionario_traducciones.columns:
        raise ValueError(f'El DataFrame correspondiente al campo ruta_csv_diccionario_traducciones debe contener la columna {columna_diccionario_traducciones_nombres}')
    if columna_diccionario_traducciones_alias not in diccionario_traducciones.columns:
        raise ValueError(f'El DataFrame correspondiente al campo ruta_csv_diccionario_traducciones debe contener la columna {columna_diccionario_traducciones_alias}')
    
    # inicializar procesador
    procesador = Procesador(
        dataframes_escalas=dataframes_escalas, 
        diccionario_traducciones=diccionario_traducciones, 
        columna_diccionario_traducciones_nombres=columna_diccionario_traducciones_nombres,
        columna_diccionario_traducciones_alias=columna_diccionario_traducciones_alias,
        variables_identificadoras=variables_identificadoras,
        variables_excluidas_list=variables_excluidas_list, 
        variables_excluidas_regex=variables_excluidas_regex 
    )
    
    # procesar variables especificadas por listas explicitas
    procesamiento_listas = pd.DataFrame()
    if variables_a_procesar_list is not None:
        if 'None' in variables_a_procesar_list:
            variables_a_procesar_list[None] = variables_a_procesar_list.pop('None')
        procesamiento_listas_dict = procesador.procesar_multiples_variables_list(escalas=list(dataframes_escalas.keys()), dicc=variables_a_procesar_list, q=q)
        procesamiento_listas = pd.concat(
            [df for df in procesamiento_listas_dict.values() if df is not None]
        ) if any(df is not None for df in procesamiento_listas_dict.values()) else pd.DataFrame()
        
    # procesar variables especificadas por listas de expresiones regulares
    procesamiento_regex = pd.DataFrame()
    if variables_a_procesar_regex is not None:
        if 'None' in variables_a_procesar_regex:
            variables_a_procesar_regex[None] = variables_a_procesar_regex.pop('None')
        procesamiento_regex_dict = procesador.procesar_multiples_variables_regex(escalas=list(dataframes_escalas.keys()), dicc=variables_a_procesar_regex, q=q)
        procesamiento_regex = pd.concat(
            [df for df in procesamiento_regex_dict.values() if df is not None]
        ) if any(df is not None for df in procesamiento_regex_dict.values()) else pd.DataFrame()

    # combinar resultados de ambos tipos de procesamiento
    resultado = pd.DataFrame()
    if variables_a_procesar_list and variables_a_procesar_regex:
        resultado = pd.concat([procesamiento_regex, procesamiento_listas])
    elif variables_a_procesar_list:
        resultado = procesamiento_listas
    elif variables_a_procesar_regex:
        resultado = procesamiento_regex
        
    # verificar duplicados en resultado
    duplicados = resultado.duplicated(['code', 'bin'])

    # imprimir advertencia de duplicados en resultado
    if duplicados.any():
        print('Advertencia: el resultado del procesamiento contiene categorías duplicadas:')
        print(resultado.loc[duplicados, ['code', 'bin']])
        
    # guardar resultado en archivo csv segun ruta_csv_salida
    resultado.to_csv(ruta_csv_salida, index=False)
    print(f'Procesamiento finalizado, el archivo .csv resultante se encuentra en la ruta:\n{ruta_csv_salida}')
    
    # imprimir tiempo total de ejecucion
    timestamp_fin = time.time()
    print(f"Tiempo total de ejecución: {timestamp_fin - timestamp_inicio:.2f} segundos")