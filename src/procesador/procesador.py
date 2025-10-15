import os
import numpy as np
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import re
from typing import Optional
from utils.regex_utils import obtener_variables_regex_df

class Procesador:
    """
    Clase para procesar variables de múltiples DataFrames asociados a diferentes escalas,
    realizando operaciones de normalización y categorización, para generar un nuevo archivo como resultado.
    Todos DataFrames que toma como atributos esta clase son generados por la clase Preprocesador.
    """
    def __init__(self, dataframes_escalas:dict, diccionario_traducciones:pd.DataFrame, columna_diccionario_traducciones_nombres:str, columna_diccionario_traducciones_alias:str, variables_identificadoras:dict, variables_excluidas_list:list, variables_excluidas_regex:list):
        """
        Inicializa el procesador validando los parámetros y configurando las variables a excluir,
        así como el diccionario de traducciones.

        Args:
            dataframes_escalas (dict): Diccionario {escala: DataFrame}.
            diccionario_traducciones (pd.DataFrame): DataFrame con traducciones de variables.
            columna_diccionario_traducciones_nombres (str): Nombre de columna con nombres descriptivos.
            columna_diccionario_traducciones_alias (str): Nombre de columna con alias de variables.
            variables_identificadoras (dict): Diccionario de variables identificadoras por escala.
            variables_excluidas_list (list): Lista de variables a excluir (explícitamente).
            variables_excluidas_regex (list): Lista de variables a excluir (por expresión regular).

        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los parámetros no son válidos.
        """
        # validaciones dataframes_escalas
        if not isinstance(dataframes_escalas, dict):
            raise TypeError('El valor del parámetro dataframes_escalas debe ser de tipo dict')
        if not all(isinstance(llave, str) for llave in dataframes_escalas.keys()):
            raise TypeError('Las llaves del diccionario dataframes_escalas deben ser de tipo str')
        if not all(isinstance(valor, pd.DataFrame) for valor in dataframes_escalas.values()):
            raise TypeError('Los valores del diccionario dataframes_escalas deben ser de tipo pd.DataFrame')
        
        # validaciones diccionario_traducciones
        if not isinstance(diccionario_traducciones, pd.DataFrame):
            raise TypeError('El valor del parámetro diccionario_variables debe ser de tipo dict')
        
        # validaciones columnas_diccionario_traducciones_nombres y columnas_diccionario_traducciones_alias
        if not isinstance(columna_diccionario_traducciones_nombres, str):
            raise TypeError('El valor del parámetro columna_diccionario_traducciones_nombres debe ser de tipo str')
        if not isinstance(columna_diccionario_traducciones_alias, str):
            raise TypeError('El valor del parámetro columna_diccionario_traducciones_alias debe ser de tipo str')
        if columna_diccionario_traducciones_nombres not in diccionario_traducciones.columns:
            raise ValueError(f'El DataFrame de diccionario_traducciones debe contener una columna con nombre {columna_diccionario_traducciones_nombres} para identificar el nombre descriptivo de cada variable')
        if columna_diccionario_traducciones_alias not in diccionario_traducciones.columns:
            raise ValueError(f'El DataFrame de diccionario_traducciones debe contener una columna con nombre {columna_diccionario_traducciones_alias} para identificar el alias de cada variable')
        
        # validaciones variables_identificadoras
        if not isinstance(variables_identificadoras, dict):
            raise TypeError('El parámetro variables_identificadoras debe ser un dict {escala: [vars]}')
        for escala, vars_id in variables_identificadoras.items():
            if escala not in dataframes_escalas:
                raise ValueError(f'La escala {escala} de variables_identificadoras no está en los dataframes')
            if not isinstance(vars_id, list):
                raise TypeError(f'Las variables identificadoras de la escala {escala} deben ser una lista')
            for var in vars_id:
                if var not in dataframes_escalas[escala].columns:
                    raise ValueError(f'La variable identificadora {var} no está en el DataFrame de la escala {escala}')
        self.variables_identificadoras = variables_identificadoras
                
        # validaciones variables_excluidas_list
        if not (isinstance(variables_excluidas_list, list)):
            raise TypeError('El valor del parámetro variables_excluidas_list debe ser de tipo list')
        if not (all(isinstance(x, str) for x in variables_excluidas_list)):
            raise TypeError('Los elementos de la lista variables_excluidas_list deben ser de tipo str')
        
        # validaciones variables_excluidas_regex
        if not (isinstance(variables_excluidas_regex, list)):
            raise TypeError('El valor del parámetro variables_excluidas_regex debe ser de tipo list')
        for regex in variables_excluidas_regex:
            try:
                re.compile(regex)
            except re.error:
                raise ValueError(f'Los elementos de la lista variables_excluidas_regex deben ser expresiones regular válidas, es inválida: {regex}')
        
        # asignacion de atributos
        self.dataframes_escalas = dataframes_escalas
        self.diccionario_traducciones = diccionario_traducciones
        self.diccionario_traducciones = dict(zip(diccionario_traducciones[columna_diccionario_traducciones_alias], diccionario_traducciones[columna_diccionario_traducciones_nombres]))
        self.variables_identificadoras = variables_identificadoras
        self.variables_excluidas = set(variables_excluidas_list)
        self.variables_faltantes_diccionario = []
        
        # imprimir en terminal variables excluidas por lista explicita
        print(f'Variables a excluir por lista explícita: {variables_excluidas_list}')
        
        # agregar variables excluidas a set segun las regex especificadas e imprimir en terminal
        variables_excluidas_encontradas_regex = set()
        for regex in variables_excluidas_regex:
            for dataframe in dataframes_escalas.values():
                variables_excluidas_encontradas_regex |= set(obtener_variables_regex_df(dataframe, regex))
        variables_excluidas_encontradas_regex = sorted(list(variables_excluidas_encontradas_regex))
        print(f'Variables a excluir encontradas por lista de regex: {variables_excluidas_encontradas_regex}')
            
        # verificar variables faltantes en el diccionario de traducciones
        self.variables_faltantes_diccionario = []
        variables_consideradas = set()
        for dataframe in dataframes_escalas.values():
            variables_consideradas = variables_consideradas | set(dataframe.columns)
        variables_en_diccionario = set(self.diccionario_traducciones.keys())
        
        # si se encuentran variables en el dataframe que no existen en el diccionario de traducciones,
        # se agregan al set de excluidas, se guardan en el atributo self.variables_faltantes_diccionario
        if not (variables_consideradas).issubset(variables_en_diccionario):
            variables_faltantes = variables_consideradas - variables_en_diccionario
            self.variables_excluidas = self.variables_excluidas | variables_faltantes
            self.variables_faltantes_diccionario = variables_faltantes
        
        # eliminar variables excluidas de los DataFrames excepto identificadoras
        variables_identificadoras_set = set([var for lista in variables_identificadoras.values() for var in lista])
        for escala, df in self.dataframes_escalas.items():
            self.dataframes_escalas[escala] = df.drop(columns=[col for col in self.variables_excluidas if col in df.columns and col not in variables_identificadoras_set])
            
        # convertir sets en listas ordenadas
        self.variables_excluidas = sorted(list(self.variables_excluidas))
        self.variables_faltantes_diccionario = sorted(list(self.variables_faltantes_diccionario))
        
        # imprimir en terminal variables faltantes en el diccionario
        print(f'Variables del DataFrame faltantes en el diccionario de traducciones (se excluirán automáticamente): {self.variables_faltantes_diccionario}')

    def get_variables_excluidas(self) -> list:
        """
        Obtiene la lista de variables excluidas.

        Returns:
            list: Lista de variables excluidas.
        """
        return list(self.variables_excluidas)

    def get_variables_faltantes_diccionario(self) -> list:
        """
        Obtiene la lista de variables faltantes en el diccionario de traducciones.

        Returns:
            list: Lista de variables faltantes en el diccionario.
        """
        return list(self.variables_faltantes_diccionario)
    
    def normalizar_variable(self, escala:str, var:str, var_base_normalizacion:Optional[str]=None) -> pd.Series:
        """
        Normaliza una variable dividiendo sus valores entre los valores de una variable base de normalización.

        Args:
            escala (str): Nombre de la escala (DataFrame) donde se encuentra la variable.
            var (str): Nombre de la variable a normalizar.
            var_base_normalizacion (str, optional): Nombre de la variable base para la normalización. Si es None, no se realiza la división.

        Returns:
            pd.Series: Serie con los valores normalizados.

        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los parámetros no son válidos.
        """
        # validaciones escala
        if not isinstance(escala, str):
            raise TypeError('El parámetro escala debe ser de tipo str')
        if escala not in self.dataframes_escalas.keys():
            raise ValueError('La escala especificada no es válida')
        
        # dataframe correspondiente a la escala
        df = self.dataframes_escalas[escala]
        
        # validaciones var
        if not isinstance(var, str):
            raise TypeError('El parámetro var debe ser de tipo str')
        if var not in df.columns:
            raise ValueError(f'La variable {var} no existe en el DataFrame de la escala especificada')
        if var in self.variables_excluidas:
            raise ValueError(f'La variable {var} está en la lista de variables excluidas')
        
        # validaciones var_base_normalizacion
        if var_base_normalizacion is not None:
            if not isinstance(var_base_normalizacion, str):
                raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str o None')
            if var_base_normalizacion not in df.columns:
                raise ValueError(f'La variable {var_base_normalizacion} no existe en el DataFrame de la escala especificada')
            if var_base_normalizacion in self.variables_excluidas:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas')
            
            # reemplazar valores de cero en la variable base por NaN para evitar divisiones por cero
            if df[var_base_normalizacion].eq(0).any():
                print(f'Advertencia: La variable {var_base_normalizacion} contiene valores de cero, reemplazando por NaN para evitar potenciales divisiones entre cero')
                df[var_base_normalizacion] = df[var_base_normalizacion].replace(0, np.nan)
                
        # dividir la variable entre la base, si no se especifica no se aplica ninuna operacion
        return df[var] / df[var_base_normalizacion] if var_base_normalizacion is not None else df[var]
    
    def categorizar_variable(self, escala:str, var:str, var_base_normalizacion:Optional[str]=None, q: int=10) -> pd.Series:
        """
        Categorización de una variable en cuantiles, con la opción de normalizarla previamente.

        Args:
            escala (str): Nombre de la escala (DataFrame) donde se encuentra la variable.
            var (str): Nombre de la variable a categorizar.
            var_base_normalizacion (str, optional): Nombre de la variable base para la normalización. Si es None, no se realiza la normalización.
            q (int): Número de cuantiles para la categorización.

        Returns:
            pd.Series: Serie categorizada en cuantiles.

        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los parámetros no son válidos.
        """
        # validaciones escala
        if not isinstance(escala, str):
            raise TypeError('El parámetro escala debe ser de tipo str')
        if escala not in self.dataframes_escalas.keys():
            raise ValueError('La escala especificada no es válida')
        
        # dataframe correspondiente a la escala
        df = self.dataframes_escalas[escala]
        
        # validaciones var
        if not isinstance(var, str):
            raise TypeError('El parámetro var debe ser de tipo str')
        if var not in df.columns:
            raise ValueError(f'La variable {var} no existe en el DataFrame de la escala especificada')
        if var in self.variables_excluidas:
            raise ValueError(f'La variable {var} está en la lista de variables excluidas')
        
        # validaciones var_base_normalizacion
        if var_base_normalizacion is not None:
            if not isinstance(var_base_normalizacion, str):
                raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str o None')
            if var_base_normalizacion not in df.columns:
                raise ValueError(f'La variable {var_base_normalizacion} no existe en el DataFrame de la escala especificada')
            if var_base_normalizacion in self.variables_excluidas:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas')
        
        # validaciones q
        if not isinstance(q, int):
            raise TypeError('El parámetro q debe ser de tipo int')
        if q < 1:
            raise ValueError('El valor de q debe ser mayor a 1')
        
        # normalizar la variable si se especifica una base de normalización, y categorizarla en cuantiles
        return pd.qcut(self.normalizar_variable(escala=escala, var=var, var_base_normalizacion=var_base_normalizacion), q=q, duplicates='drop')
    
    def __list_a_postgres_array(self, lista:list) -> str:
        """
        Convierte una lista en un formato compatible con un array de PostgreSQL.

        Args:
            lista (list): Lista a convertir.

        Returns:
            str: Cadena con el formato de array de PostgreSQL.
        """
        # validaciones lista
        if not isinstance(lista, list) and not all(isinstance(x, str) for x in lista):
            raise TypeError('El parámetro list debe ser de tipo list, y cada elemento debe ser de tipo str')
            
        # eliminar comillas simples y dobles de los elementos de la lista
        clean = [str(x).replace("'", "").replace('"', "") for x in lista]
        
        return '{' + ','.join(f'{c}' for c in lista) + '}'
    
    def procesar_variable(self, escalas:list, var:str, var_base_normalizacion:Optional[str]=None, q:int=10) -> pd.DataFrame:
        """
        Procesa una variable específica en las escalas especificadas, normalizándola y categorizándola en cuantiles.

        Args:
            escalas (list): Lista de escalas (DataFrames) donde se procesará la variable.
            var (str): Nombre de la variable a procesar.
            var_base_normalizacion (str, optional): Nombre de la variable base para la normalización. Si es None, no se realiza la normalización.
            q (int): Número de cuantiles para la categorización.

        Returns:
            pd.DataFrame: DataFrame con los resultados del procesamiento de la variable.

        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los parámetros no son válidos.
        """
        # validaciones escalas
        if not isinstance(escalas, list):
            raise TypeError('El parámetro escalas debe ser de tipo list')
        for escala in escalas:
            if not isinstance(escala, str):
                raise TypeError('Los elementos del parámetro escalas deben ser de tipo str')
            if escala not in self.dataframes_escalas.keys():
                raise ValueError(f'La escala {escala} no es válida, pues no fue proporcionado un DataFrame para esta')
        
        # validaciones var
        if not isinstance(var, str):
            raise TypeError('El parámetro var debe ser de tipo str')
        if var in self.variables_identificadoras:
            raise ValueError(f'La variable {var} está en la lista de variables identificadoras, no se procesará')
        if var in self.variables_excluidas:
            raise ValueError(f'La variable {var} está en la lista de variables excluidas')
        
        # validaciones var_base_normalizacion
        if var_base_normalizacion is not None:
            if not isinstance(var_base_normalizacion, str):
                raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str o None')
            if var_base_normalizacion in self.variables_identificadoras:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables identificadoras, no se procesará')
            if var_base_normalizacion in self.variables_excluidas:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas, no se procesará')
        
        # validaciones q
        if not isinstance(q, int):
            raise TypeError('El parámetro q debe ser de tipo int')
        if q < 1:
            raise ValueError('El valor de q debe ser mayor a 1')
        
        # inicializar diccionario para almacenar los resultados
        resultado = {
            'name': [],
            'code': [],
            'bin': []
        }
        
        # inicializar diccionarios para verificar si var y var_base_normalizacion existen en las escalas,
        # cada escala es una llave, y el valor es True o False dependiendo si la variable se encuentra en el dataframe de la escala
        validacion_escalas_var = {escala:False for escala in escalas}
        validacion_escalas_var_base_normalizacion = {escala:False for escala in escalas}
        
        for escala in escalas:
            
            # inicializar las columnas de intervalos y datos pertenecientes a los intervalos para cada escala
            resultado[f'interval_{escala}'] = []
            resultado[f'cells_{escala}'] = []
            
            # validar si var existe en el dataframe de la escala actual
            if var not in self.dataframes_escalas[escala].columns:
                print(f'La variable {var} (var) no existe en el DataFrame de la escala {escala}')
            else:
                validacion_escalas_var[escala] = True
                
            # validar si var_base_normalizacion existe en el dataframe de la escala
            # (solo cuando no es None, en caso contrario se asigna True por defecto a todas las llaves del diccionario)
            if var_base_normalizacion is not None:
                if var_base_normalizacion not in self.dataframes_escalas[escala].columns:
                    print(f'La variable {var_base_normalizacion} (var_base_normalizacion) no existe en el DataFrame de la escala {escala}')
                else:
                    validacion_escalas_var_base_normalizacion[escala] = True
            else:
                validacion_escalas_var_base_normalizacion[escala] = True
        
        # si var no existe en ninguna escala (todos los valores del diccionario correspondiente son False) lanzar un error
        if all(validacion_escalas_var[escala] == False for escala in escalas):
            raise ValueError(f'La variable {var} (var) no existe en ninguno de los DataFrames de las escalas especificadas')
        
        # si var_base_normalizacion no existe en ninguna escala (no es None, y todos los valores del diccionario son False) lanzar un error
        if all(validacion_escalas_var_base_normalizacion[escala] == False for escala in escalas):
            raise ValueError(f'La variable {var_base_normalizacion} (var_base_normalizacion) no existe en ninguno de los DataFrames de las escalas especificadas')
        
        # obtener el nombre y código de la variable desde el diccionario de traducciones
        nombre = self.diccionario_traducciones[var]
        codigo = var
        
        # inicializar diccionario para almacenar la lista de intervalos generados en cada escala de manera ordenada
        # el diccionario intervalos_ordenados_escalas se va a ver de la siguiente forma: 
        # {
        #     escala_1:[intervalo_1, intervalo_2, ...],
        #     escala_2:[intervalo_1], intervalo_2, ...],
        #     ...   
        # }
        intervalos_ordenados_escalas = {escala:[] for escala in escalas}
        
        # inicializar diccionario para almacenar los datos pertenecientes a cada intervalo
        # el diccionario intervalos_cells_escalas se va a ver de la siguiente forma:
        # {
        #     escala_1: 
        #         {
        #             intervalo_1:[entidad_a, entidad_x, ...],
        #             intervalo_2:[entidad_b, entidad_y, ...],
        #             ...
        #         },
        #     escala_2:
        #         {
        #             intervalo_1:[entidad_a, entidad_x, ...],
        #             intervalo_2:[entidad_b, entidad_y, ...],
        #             ... 
        #         },
        #     ...
        # }
        intervalos_cells_escalas = {escala:{} for escala in escalas}
        
        for escala in escalas:
            
            # comprobar si var y var_base_normalizacion (cuando no es None) existen en el dataframe de la escala actual
            if validacion_escalas_var[escala] and validacion_escalas_var_base_normalizacion[escala]:
                
                # categorizar variable en cuantiles
                variable_categorizada = self.categorizar_variable(escala=escala, var=var, var_base_normalizacion=var_base_normalizacion, q=q)
                
                # agregar una categoría para valores NaN
                variable_categorizada = variable_categorizada.cat.add_categories(['NaN'])
                variable_categorizada = variable_categorizada.fillna('NaN')
                
                # inicializar un diccionario para almacenar las listas de datos pertenecientes a cada intervalo generado en la escala actual,
                # este diccionario se almacenara en el diccionario intervalos_cells_escalas para cada variable
                cells = {intervalo: [] for intervalo in variable_categorizada.cat.categories}
                
                for i, intervalo in enumerate(variable_categorizada):
                    
                    # construir la cadena que identifica al dato a partir de las variables identificadoras
                    # y agregarla a la lista de datos que pertenecen al intervalo
                    entidad = "".join(str(self.dataframes_escalas[escala].iloc[i][col]) for col in self.variables_identificadoras[escala])
                    cells[intervalo].append(entidad)
                                    
                # eliminar la categoria NaN si no contiene valores
                if len(cells['NaN']) == 0:
                    variable_categorizada = variable_categorizada.cat.remove_categories(['NaN'])
                    del cells['NaN']
                    
                # almacenar cells en el diccionario intervalos_cells_escalas en la escala actual
                intervalos_cells_escalas[escala] = cells
                
                # almacenar la lista de intervalos generados para la variable de manera ordenada en el diccionario intervalos_ordenados_escalas en la escala actual
                intervalos_ordenados_escalas[escala] = sorted(variable_categorizada.value_counts().index, key=lambda x: (isinstance(x, str), x))

        # el resultado consistira de a lo mas q+1 bins (entradas) considerando la categoria NaN
        # se eliminaran los bins donde ninguna escala haya generado un intervalo en ese bin (se tienen valores nulos pd.NA en las columnas de ese bin)
        for i in range(q+1):
            
            # agregar name, code y bin al resultado
            resultado['name'].append(nombre)
            resultado['code'].append(codigo)
            resultado['bin'].append(i+1)
            
            for escala in escalas:
                
                try:
                    
                    # obtener el intervalo correspondiente al bin actual
                    intervalo = intervalos_ordenados_escalas[escala][i]
                    
                    # agregar el intervalo (o categoria NaN) al resultado
                    if isinstance(intervalo, pd.Interval):
                        resultado[f'interval_{escala}'].append(
                            f'{(intervalo.left*100).round(1)}%:{(intervalo.right*100).round(1)}%' if (var_base_normalizacion is not None) else f'{intervalo.left}:{intervalo.right}'
                        )
                    else:
                        resultado[f'interval_{escala}'].append('Sin clasificar')
                        
                    # agregar las celdas correspondientes al intervalo
                    resultado[f'cells_{escala}'].append(self.__list_a_postgres_array(intervalos_cells_escalas[escala][intervalo]))
                    
                except IndexError:
                    
                    # si no hay más intervalos, agregar valores nulos
                    resultado[f'interval_{escala}'].append(pd.NA)
                    resultado[f'cells_{escala}'].append(pd.NA)
        
        # filtrar y eliminan los bins en donde todos sus valores para las columnas de intervalos y cells son nulos
        df_resultado = pd.DataFrame(resultado)
        columnas_a_validar = [col for col in df_resultado.columns if col.startswith('interval_') or col.startswith('cells_')]
        bins_nulos = df_resultado[columnas_a_validar].isna().all(axis=1)
        
        return df_resultado[~bins_nulos].reset_index(drop=True)
    
    def procesar_multiples_variables_list(self, escalas:list, dicc:dict, q:int=10) -> dict:
        """
        Procesa múltiples variables utilizando listas explícitas de nombres de variables.

        Args:
            escalas (list): Lista de escalas (DataFrames) donde se procesarán las variables.
            dicc (dict): Diccionario donde las llaves son bases de normalización y los valores son listas de variables.
            q (int): Número de cuantiles para la categorización.

        Returns:
            dict: Diccionario con los resultados del procesamiento para cada base de normalización.

        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los parámetros no cumplen con los requisitos.
        """
        # validaciones escalas
        if not isinstance(escalas, list):
            raise TypeError('El parámetro escalas debe ser de tipo list')
        for escala in escalas:
            if not isinstance(escala, str):
                raise TypeError('Los elementos del parámetro escalas deben ser de tipo str')
            if escala not in self.dataframes_escalas.keys():
                raise ValueError(f'La escala {escala} no es válida, pues no fue proporcionado un DataFrame para esta')
            
        # validaciones dicc
        if not isinstance(dicc, dict):
            raise TypeError('El valor del parámetro dicc debe ser de tipo dict, de forma {base_porcentaje : lista_variables}')
        if dicc == {}:
            raise ValueError('El diccionario no contiene items, debe ser un diccionario de forma {base_porcentaje : lista variables}')
        
        # validaciones q
        if not isinstance(q, int):
            raise TypeError('El parámetro q debe ser de tipo int')
        if q < 1:
            raise ValueError('El valor de q debe ser mayor a 1')
        
         # inicializar el diccionario de resultados
        resultado = {}
        
        for var_base_normalizacion, lista_vars in dicc.items():
            
            # validaciones llaves dicc (var_base_normalizacion)
            if var_base_normalizacion is not None:
                if not isinstance(var_base_normalizacion, str):
                    raise TypeError('Las llaves del diccionario deben ser de tipo str o None')
                if var_base_normalizacion in self.variables_identificadoras:
                    print(f"Advertencia: La variable seleccionada como base de normalización {var_base_normalizacion} (con lista explícita) está en la lista de variables identificadoras, no se procesará la lista asociada a esta variable")
                    continue
                if var_base_normalizacion in self.variables_excluidas:
                    print(f"Advertencia: La variable seleccionada como base de normalización {var_base_normalizacion} (con lista explícita) está en la lista de variables excluidas, no se procesará la lista asociada a esta variable")
                    continue
                
            # validaciones valores dicc (lista explicita de variables a procesar para la var_base_normalizacion correspondiente)
            if not isinstance(lista_vars, list):
                raise TypeError('Los valores del diccionario deben ser de tipo list')
            
            # imprimir en terminal el total de variables a procesar
            totales_por_base = len(lista_vars)
            print(f"Total de variables a procesar para base de normalización '{var_base_normalizacion}' (con lista explícita): {totales_por_base}")
            
            # inicializar lista de procesamientos para cada variable
            variables_procesadas = []
            
            # inicializar contador de variables procesadas
            contador = 0
            
            for var in lista_vars:
                
                # validaciones elementos de lista_vars (variable individual a procesar)
                if not isinstance(var, str):
                    raise TypeError('Los elementos de las listas deben ser de tipo str')
                
                # aumentar en uno contador de variables procesadas 
                contador += 1
                
                # validar que la variable no este en las identificadoras o excluidas
                if var in self.variables_identificadoras:
                    print(f"- (Base: '{var_base_normalizacion}') {contador}/{totales_por_base} La variable '{var}' está en la lista de variables identificadoras, no se procesará")
                    continue
                if var in self.variables_excluidas:
                    print(f"- (Base: '{var_base_normalizacion}') {contador}/{totales_por_base} La variable '{var}' está en la lista de variables excluidas, no se procesará")
                    continue
                
                # imprimir en terminal variable actual y progreso con respecto al total de variables
                print(f"- (Base: '{var_base_normalizacion}') Procesando variable {contador}/{totales_por_base}: '{var}'")
                
                # procesar la variable y agregarla a la lista de resultados
                variables_procesadas.append(self.procesar_variable(escalas=escalas, var=var, var_base_normalizacion=var_base_normalizacion, q=q))
            
            # concatenar los resultados en un solo dataframe y agregarlos al diccionario resultados segun la variable usada como base de normalizacion
            resultado[var_base_normalizacion] = pd.concat(variables_procesadas, ignore_index=True) if len(variables_procesadas) > 0 else None
            
        return resultado
    
    def procesar_multiples_variables_regex(self, escalas:list, dicc:dict, q:int=10) -> dict:
        """
        Procesa múltiples variables utilizando expresiones regulares para seleccionarlas.

        Args:
            escalas (list): Lista de escalas (DataFrames) en las que se procesarán las variables.
            dicc (dict): Diccionario donde las llaves son bases de normalización y los valores son listas de expresiones regulares.
            q (int): Número de cuantiles para la categorización.

        Returns:
            dict: Diccionario con los resultados del procesamiento para cada base de normalización.

        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los parámetros no cumplen con los requisitos.
        """
        
        # validaciones escalas
        if not isinstance(escalas, list):
            raise TypeError('El parámetro escalas debe ser de tipo list')
        for escala in escalas:
            if not isinstance(escala, str):
                raise TypeError('Los elementos del parámetro escalas deben ser de tipo str')
            if escala not in self.dataframes_escalas.keys():
                raise ValueError(f'La escala {escala} no es válida, pues no fue proporcionado un DataFrame para esta')
            
        # validaciones dicc
        if not isinstance(dicc, dict):
            raise TypeError('El valor del parámetro dicc debe ser de tipo dict, de forma {base_porcentaje : lista_variables}')
        if dicc == {}:
            raise ValueError('El diccionario no contiene items, debe ser un diccionario de forma {base_porcentaje : lista variables}')
        
        # validaciones q
        if not isinstance(q, int):
            raise TypeError('El parámetro q debe ser de tipo int')
        if q < 1:
            raise ValueError('El valor de q debe ser mayor a 1')
        
        # inicializar diccionario de resultados
        resultado = {}
        
        for var_base_normalizacion, regex_list in dicc.items():
            
            # validaciones llaves dicc (var_base_normalizacion)
            if var_base_normalizacion is not None:
                if not isinstance(var_base_normalizacion, str):
                    raise TypeError('Las llaves del diccionario deben ser de tipo str o None')
                if var_base_normalizacion in self.variables_identificadoras:
                    print(f"La variable seleccionada como base de normalización {var_base_normalizacion} está en la lista de variables identificadoras, no se procesará la lista asociada a esta variable")
                    continue
                if var_base_normalizacion in self.variables_excluidas:
                    print(f"La variable seleccionada como base de normalización {var_base_normalizacion} está en la lista de variables excluidas, no se procesará la lista asociada a esta variable")
                    continue
                
            # validaciones valores dicc (lista de expresiones regulares para la var_base_normalizacion correspondiente)
            if not isinstance(regex_list, list) or not all(isinstance(r, str) for r in regex_list):
                raise TypeError('Cada valor del diccionario debe ser una lista de strings (regex)')
            for regex in regex_list:
                try:
                    re.compile(regex)
                except re.error:
                    raise ValueError(f'La expresión regular {regex} no es válida')
            
            # combinar todas las regex en una sola
            regex_combinada = "|".join(f"(?:{r})" for r in regex_list)
            
            # inicializar el contador de variables procesadas
            totales_por_base = 0
            
            # obtener las variables que coinciden con la regex combinada en los dataframes
            variables_obtenidas_regex = set()
            for escala in escalas:
                # evaluar regex combinada en cada dataframe de las escalas especificadas, y agregar resultados al set variables_obtenidas_regex
                variables_obtenidas_regex |= set(obtener_variables_regex_df(self.dataframes_escalas[escala], regex_combinada))
                totales_por_base += len(variables_obtenidas_regex)
                
            # si no se obtuvo ningun resultado de la regex combinada, se omite esa var_base_normalizacion
            if len(variables_obtenidas_regex) == 0:
                print(f'Ninguna expresión regular en la lista {regex_list} coincide con alguna variable en los DataFrames, omitiendo')
                continue
            
            # imprimir en terminal el total de variables a procesar segun la evaluacion de la regex combinada
            print(f"Total de variables a procesar para base de normalización '{var_base_normalizacion}' (con lista de expresiones regulares): {totales_por_base}")
            
            # inicializar lista de procesamientos para cada variable
            variables_procesadas_regex_list = []
            
            # inicializar contador de variables procesadas
            contador = 0
            
            for var in sorted(list(variables_obtenidas_regex)):
                
                # aumentar en uno contador de variables procesadas 
                contador += 1
                
                # validar que la variable no este en las identificadoras o excluidas
                if var in self.variables_identificadoras:
                    print(f"- (Base: '{var_base_normalizacion}') {contador}/{totales_por_base} La variable '{var}' está en la lista de variables identificadoras, no se procesará")
                    continue
                if var in self.variables_excluidas:
                    print(f"- (Base: '{var_base_normalizacion}') {contador}/{totales_por_base} La variable '{var}' está en la lista de variables excluidas, no se procesará")
                    continue
                
                # imprimir en terminal variable actual y progreso con respecto al total de variables
                print(f"- (Base: '{var_base_normalizacion}') Procesando variable {contador}/{totales_por_base}: '{var}'")
                
                # procesar la variable y agregarla a la lista de resultados
                variables_procesadas_regex_list.append(self.procesar_variable(escalas=escalas, var=var, var_base_normalizacion=var_base_normalizacion, q=q))
            
            # concatenar los resultados en un solo dataframe y agregarlos al diccionario resultados segun la variable usada como base de normalizacion
            resultado[var_base_normalizacion] = pd.concat(variables_procesadas_regex_list, ignore_index=True) if len(variables_procesadas_regex_list) > 0 else None
        
        return resultado