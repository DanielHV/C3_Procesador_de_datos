import os
import numpy as np
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import re

class Procesador:

    def __init__(self, diccionario_variables:dict, dataframes_escalas:dict, variables_excluidas:list, variable_identificadora:str):
        
        if not isinstance(diccionario_variables, dict):
            raise TypeError('El valor del parámetro diccionario_variables debe ser de tipo dict')
        if not all(isinstance(llave, str) for llave in diccionario_variables.keys()):
            raise TypeError('Las llaves del diccionario diccionario_variables deben ser de tipo str')
        if not all(isinstance(valor, str) for valor in diccionario_variables.values()):
            raise TypeError('Los valores del diccionario diccionario_variables deben ser de tipo str')
        
        if not isinstance(dataframes_escalas, dict):
            raise TypeError('El valor del parámetro dataframes_escalas debe ser de tipo dict')
        if not all(isinstance(llave, str) for llave in dataframes_escalas.keys()):
            raise TypeError('Las llaves del diccionario dataframes_escalas deben ser de tipo str')
        if not all(isinstance(valor, pd.DataFrame) for valor in dataframes_escalas.values()):
            raise TypeError('Los valores del diccionario dataframes_escalas deben ser de tipo pd.DataFrame')
                
        if not (isinstance(variables_excluidas, list)):
            raise TypeError('El valor del parámetro variables_excluidas debe ser de tipo list')
        if not (all(isinstance(x, str) for x in variables_excluidas)):
            raise TypeError('Los elementos de la lista variables_excluidas deben ser de tipo str')
        
        if not isinstance(variable_identificadora, str):
            raise TypeError('El valor del parámetro variable_identificadora debe ser de tipo str')
        
        self.diccionario_variables = diccionario_variables
        self.dataframes_escalas = dataframes_escalas
        self.variables_excluidas = variables_excluidas
        self.variable_identificadora = variable_identificadora
        
        variables_consideradas = set()
        for dataframe in dataframes_escalas.values():
            variables_consideradas = variables_consideradas | set(dataframe.columns)
            
        variables_consideradas = variables_consideradas - set(self.variables_excluidas)

        variables_en_diccionario = set(diccionario_variables.keys())
        if not (variables_consideradas).issubset(variables_en_diccionario):
            variables_faltantes = variables_consideradas - variables_en_diccionario
            raise ValueError(f'Las siguientes variables numéricas del DataFrame no están presentes en las llaves del diccionario: {', '.join(variables_faltantes)}')
        
    def normalizar_variable(self, escala:str, var:str, var_base_normalizacion:str=None) -> pd.Series:
        
        if not isinstance(escala, str):
            raise TypeError('El parámetro escala debe ser de tipo str')
        if escala not in self.dataframes_escalas.keys():
            raise ValueError('La escala especificada no es válida')
        
        df = self.dataframes_escalas[escala]
        if not isinstance(var, str):
            raise TypeError('El parámetro var debe ser de tipo str')
        if var not in df.columns:
            raise ValueError(f'La variable {var} no existe en el DataFrame de la escala especificada')
        if var in self.variables_excluidas:
            raise ValueError(f'La variable {var} está en la lista de variables excluidas')
        
        if var_base_normalizacion is not None:
            if not isinstance(var_base_normalizacion, str):
                raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str o None')
            if var_base_normalizacion not in df.columns:
                raise ValueError(f'La variable {var_base_normalizacion} no existe en el DataFrame de la escala especificada')
            if var_base_normalizacion in self.variables_excluidas:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas')
        
        if df[var_base_normalizacion].eq(0).any():
            print(f'Advertencia: La variable {var_base_normalizacion} contiene valores de cero, reemplazando por NaN para evitar potenciales divisiones entre cero')
            df[var_base_normalizacion] = df[var_base_normalizacion].replace(0, np.nan)
            
        return df[var] / df[var_base_normalizacion] if var_base_normalizacion is not None else df[var]
    
    def categorizar_variable(self, escala:str, var:str, var_base_normalizacion:str=None, q:int=10) -> pd.Series:
        
        if not isinstance(escala, str):
            raise TypeError('El parámetro escala debe ser de tipo str')
        if escala not in self.dataframes_escalas.keys():
            raise ValueError('La escala especificada no es válida')
        
        df = self.dataframes_escalas[escala]
        if not isinstance(var, str):
            raise TypeError('El parámetro var debe ser de tipo str')
        if var not in df.columns:
            raise ValueError(f'La variable {var} no existe en el DataFrame de la escala especificada')
        if var in self.variables_excluidas:
            raise ValueError(f'La variable {var} está en la lista de variables excluidas')
        
        if var_base_normalizacion is not None:
            if not isinstance(var_base_normalizacion, str):
                raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str o None')
            if var_base_normalizacion not in df.columns:
                raise ValueError(f'La variable {var_base_normalizacion} no existe en el DataFrame de la escala especificada')
            if var_base_normalizacion in self.variables_excluidas:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas')
        
        if not isinstance(q, int):
            raise TypeError('El parámetro q debe ser de tipo int')
        if q < 1:
            raise ValueError('El valor de q debe ser mayor a 1')
        
        return pd.qcut(self.normalizar_variable(escala=escala, var=var, var_base_normalizacion=var_base_normalizacion), q=q, duplicates='drop')
    
    def __list_a_postgres_array(self, obj):
        
        if isinstance(obj, list):
            clean = [str(x).replace("'", "").replace('"', "") for x in obj]
            return '{' + ','.join(f'{c}' for c in clean) + '}'
        return obj
    
    def procesar_variable(self, escalas:list, var:str, var_base_normalizacion:str=None, q:int=10) -> pd.DataFrame:
        
        # validaciones de parametros
        
        if not isinstance(escalas, list):
            raise TypeError('El parámetro escalas debe ser de tipo list')
        for escala in escalas:
            if not isinstance(escala, str):
                raise TypeError('Los elementos del parámetro escalas deben ser de tipo str')
            if escala not in self.dataframes_escalas.keys():
                raise ValueError(f'La escala {escala} no es válida')
        
        if not isinstance(var, str):
            raise TypeError('El parámetro var debe ser de tipo str')
        if var in self.variables_excluidas:
            raise ValueError(f'La variable {var} está en la lista de variables excluidas')
        
        if var_base_normalizacion is not None:
            if not isinstance(var_base_normalizacion, str):
                raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str o None')
            if var_base_normalizacion not in df.columns:
                raise ValueError(f'La variable {var_base_normalizacion} no existe en el DataFrame de la escala especificada')
            if var_base_normalizacion in self.variables_excluidas:
                raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas')
        
        if not isinstance(q, int):
            raise TypeError('El parámetro q debe ser de tipo int')
        if q < 1:
            raise ValueError('El valor de q debe ser mayor a 1')
        
        resultado = {
            'name': [],
            'code': [],
            'bin': []
        }
        
        nombre = self.diccionario_variables[var]
        codigo = var
        
        # se valida si la variable a procesar existe en al menos un dataframe de las escalas especificadas
        
        validacion_escalas_var = {escala:False for escala in escalas}
        validacion_escalas_var_base_normalizacion = {escala:False for escala in escalas}
        for escala in escalas:
            
            resultado[f'interval_{escala}'] = []
            resultado[f'cells_{escala}'] = []
            
            if var not in self.dataframes_escalas[escala].columns:
                print(f'La variable {var} (var) no existe en el DataFrame de la escala {escala}')
            else:
                validacion_escalas_var[escala] = True
            if var_base_normalizacion is not None:
                if var_base_normalizacion not in self.dataframes_escalas[escala].columns:
                    print(f'La variable {var_base_normalizacion} (var_base_normalizacion) no existe en el DataFrame de la escala {escala}')
                else:
                    validacion_escalas_var_base_normalizacion[escala] = True
            else:
                validacion_escalas_var_base_normalizacion[escala] = True
        
        if all(validacion_escalas_var[escala] == False for escala in escalas):
            raise ValueError(f'La variable {var} (var) no existe en ninguno de los DataFrames de las escalas especificadas')
        if all(validacion_escalas_var_base_normalizacion[escala] == False for escala in escalas):
            raise ValueError(f'La variable {var_base_normalizacion} (var_base_normalizacion) no existe en ninguno de los DataFrames de las escalas especificadas')
        
        # se hace la categorizacion de la variable en los dataframes donde si se encuentra
        
        intervalos_cells_escalas = {escala:{} for escala in escalas}
        intervalos_ordenados_escalas = {escala:[] for escala in escalas}
        for escala in escalas:
            
            if validacion_escalas_var[escala] and validacion_escalas_var_base_normalizacion[escala]:
                variable_categorizada = self.categorizar_variable(escala=escala, var=var, var_base_normalizacion=var_base_normalizacion, q=q)
                variable_categorizada = variable_categorizada.cat.add_categories(['NaN'])
                variable_categorizada = variable_categorizada.fillna('NaN')
                
                cells = {intervalo:[] for intervalo in variable_categorizada.cat.categories}
                for entidad, intervalo in zip(self.dataframes_escalas[escala][self.variable_identificadora], variable_categorizada):
                    cells[intervalo].append(entidad)
                    
                if len(cells['NaN']) == 0:
                    variable_categorizada = variable_categorizada.cat.remove_categories(['NaN'])
                    del cells['NaN']
                    
                intervalos_cells_escalas[escala] = cells
                intervalos_ordenados_escalas[escala] = sorted(variable_categorizada.value_counts().index, key=lambda x: (isinstance(x, str), x))
        
        # se construye el dataframe resultante
        
        for i in range(q+1):
            resultado['name'].append(nombre)
            resultado['code'].append(codigo)
            resultado['bin'].append(i+1)
            
            for escala in escalas:
                try:
                    intervalo = intervalos_ordenados_escalas[escala][i]
                    if isinstance(intervalo, pd.Interval):
                        resultado[f'interval_{escala}'].append(
                            f'{(intervalo.left*100).round(1)}%:{(intervalo.right*100).round(1)}%' if (var_base_normalizacion is not None) else f'{intervalo.left}:{intervalo.right}'
                        )
                    else:
                        resultado[f'interval_{escala}'].append('Sin clasificar')
                    resultado[f'cells_{escala}'].append(self.__list_a_postgres_array(intervalos_cells_escalas[escala][intervalo]))
                except IndexError:
                    resultado[f'interval_{escala}'].append(pd.NA)
                    resultado[f'cells_{escala}'].append(pd.NA)
        
        # se filtran y eliminan los bins en donde todos sus valores para las columnas de intervalos y cells son nulos
        
        df_resultado = pd.DataFrame(resultado)
        columnas_a_validar = [col for col in df_resultado.columns if col.startswith('interval_') or col.startswith('cells_')]
        bins_nulos = df_resultado[columnas_a_validar].isna().all(axis=1)
        
        return df_resultado[~bins_nulos].reset_index(drop=True)
    
