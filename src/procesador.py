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
        self.variable_identificarora = variable_identificadora
        
        variables_consideradas = set()
        for dataframe in dataframes_escalas.values():
            variables_consideradas = variables_consideradas | set(dataframe.columns)
            
        variables_consideradas = variables_consideradas - set(self.variables_excluidas)

        variables_en_diccionario = set(diccionario_variables.keys())
        if not (variables_consideradas).issubset(variables_en_diccionario):
            variables_faltantes = variables_consideradas - variables_en_diccionario
            raise ValueError(f'Las siguientes variables numéricas del DataFrame no están presentes en las llaves del diccionario: {', '.join(variables_faltantes)}')
        
    def normalizar_variable(self, escala:str, var:str, var_base_normalizacion:str) -> pd.Series:
        
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
        
        if not isinstance(var_base_normalizacion, str):
            raise TypeError('El parámetro var_base_normalizacion debe ser de tipo str')
        if var_base_normalizacion not in df.columns:
            raise ValueError(f'La variable {var_base_normalizacion} no existe en el DataFrame de la escala especificada')
        if var_base_normalizacion in self.variables_excluidas:
            raise ValueError(f'La variable {var_base_normalizacion} está en la lista de variables excluidas')
        
        if df[var_base_normalizacion].eq(0).any():
            print(f'Advertencia: La variable {var_base_normalizacion} contiene valores de cero, reemplazando por NaN para evitar potenciales divisiones entre cero')
            df[var_base_normalizacion] = df[var_base_normalizacion].replace(0, np.nan)
            
        return df[var] / df[var_base_normalizacion]
    
