import pandas as pd
import ast

class Preprocesador:
    
    def __init__(self, df:pd.DataFrame, metadatos:pd.DataFrame, columna_metadatos_nombres:str, columna_metadatos_posibles_valores:str):

        if not isinstance(df, pd.DataFrame):
            raise TypeError('El valor del parámetro df debe ser de tipo pd.DataFrame')
        self.df = df
        if 'Unnamed: 0' in self.df.columns:
            self.df.drop(columns=['Unnamed: 0'], inplace=True)
        
        if not isinstance(metadatos, pd.DataFrame):
            raise TypeError('El valor del parámetro metadatos debe ser de tipo pd.DataFrame')
        self.metadatos = metadatos
        if 'Unnamed: 0' in self.metadatos.columns:
            self.metadatos.drop(columns=['Unnamed: 0'], inplace=True)
        
        if not isinstance(columna_metadatos_nombres, str):
            raise TypeError('El parámetro columna_metadatos_nombres debe ser de tipo str')
        self.columna_metadatos_nombres = columna_metadatos_nombres

        if not isinstance(columna_metadatos_posibles_valores, str):
            raise TypeError('El parámetro columna_metadatos_posibles_valores debe ser de tipo str')
        self.columna_metadatos_posibles_valores = columna_metadatos_posibles_valores
        
    def eliminar_cadenas_vacias(self):
        
        self.df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        
    def columnas_a_alias(self, columna_metadatos_alias:str):
        
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        
        diccionario_variables = dict(zip(self.metadatos[self.columna_metadatos_nombres], self.metadatos[columna_metadatos_alias]))
        self.df.rename(columns=diccionario_variables, inplace=True)
        
    def convertir_tipos(self, columna_metadatos_tipos:str):
        
        if not isinstance(columna_metadatos_tipos, str):
            raise TypeError('El parámetro columna_metadatos_tipos debe ser de tipo str')
        for var, tipo in zip(self.metadatos[self.columna_metadatos_nombres], self.metadatos[columna_metadatos_tipos]):
            try: 
                if tipo.lower() in ['int', 'int64']:
                    self.df[var] = self.df[var].astype('Int64')
                elif tipo.lower() in ['float', 'float64']:
                    self.df[var] = self.df[var].astype('Float64')
                elif tipo.lower() in ['bool', 'boolean']:
                    self.df[var] = self.df[var].astype('boolean')
                elif tipo.lower() in ['str', 'string']:
                    self.df[var] = self.df[var].astype('string')
                else:
                    self.df[var] = self.df[var].astype(tipo)
            except KeyError:
                raise KeyError(f'La variable {var} no se encuentra en el DataFrame')
            except TypeError:
                raise TypeError(f'El tipo {tipo} no es válido para la variable {var}')

    def excluir_variables(self, columna_metadatos_filtro_excluir:str, valores_a_excluir:list):

        if not isinstance(columna_metadatos_filtro_excluir, str):
            raise TypeError('El parámetro columna_metadatos_filtro_excluir debe ser de tipo str')
        if not isinstance(valores_a_excluir, list):
            raise TypeError('El parámetro valores_a_excluir debe ser de tipo list')

        metadatos_filtrados = self.metadatos.loc[~self.metadatos[columna_metadatos_filtro_excluir].isin(valores_a_excluir)]
        columnas_filtradas = [col for col in metadatos_filtrados[self.columna_metadatos_nombres] if col in self.df.columns]

        self.df = self.df[columnas_filtradas]
        
    def generar_diccionario_traducciones_variables_categoricas(self, variables:list, columna_metadatos_alias:str, columna_metadatos_posibles_valores_alias:str):

        if not isinstance(variables, list):
            raise TypeError('El parámetro variables debe ser de tipo list')
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        if not isinstance(columna_metadatos_posibles_valores_alias, str):
            raise TypeError('El parámetro columna_metadatos_posibles_valores_alias debe ser de tipo str')

        diccionario_traducciones = {}
        for variable in variables:
            fila = self.metadatos[self.metadatos[self.columna_metadatos_nombres] == variable]
            if fila.empty:
                continue
            variable_alias = fila[columna_metadatos_alias].values[0]
            posibles_valores = ast.literal_eval(fila[self.columna_metadatos_posibles_valores].values[0])
            posibles_valores_alias = ast.literal_eval(fila[columna_metadatos_posibles_valores_alias].values[0])
            
            for valor, valor_alias in zip(posibles_valores, posibles_valores_alias):
                diccionario_traducciones[f'{variable}-{valor}'] = f'{variable_alias}-{valor_alias}'

        return diccionario_traducciones
    
    
    def generar_diccionario_traducciones_variables_numericas(self, variables:list, columna_metadatos_alias:str, operacion:str):
        
        if not isinstance(variables, list):
            raise TypeError('El parámetro variables debe ser de tipo list')
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        if not isinstance(operacion, str):
            raise TypeError('El parámetro operacion debe ser de tipo str')
        
        diccionario_traducciones = {}
        for variable in variables:
            fila = self.metadatos[self.metadatos[self.columna_metadatos_nombres] == variable]
            if fila.empty:
                continue
            variable_alias = fila[columna_metadatos_alias].values[0]
            diccionario_traducciones[f'{operacion}::{variable}'] = f'{operacion}::{variable_alias}'
        
        return diccionario_traducciones
    
    
    def generar_diccionario_total_datos(self):
        return {'conteo::total_datos':'conteo::total_datos'}


    def agrupar_variables_categoricas(self, variables_id_agrupacion, variables_a_agrupar):
        
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El valor del parámetro variables_id_agrupacion debe ser de tipo list')
        
        if not isinstance(variables_a_agrupar, list):
            raise TypeError('El valor del parámetro variables_a_agrupar debe ser de tipo list')

        df = self.df[variables_id_agrupacion+variables_a_agrupar]
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

        df = df.loc[:, ~df.columns.duplicated()] # si las variables de agrupacion se encuentran repetidas en las variables a agrupar, se eliminan

        df_melt = df.melt(
            id_vars=variables_id_agrupacion,
            value_vars=variables_a_agrupar,
            var_name='característica',
            value_name='observación'
        )
        
        df_conteos = (
            df_melt
            .groupby(variables_id_agrupacion + ['característica', 'observación'])
            .size()
            .reset_index(name='conteo')
        )
        
        df_agregado = df_conteos.pivot_table(
            index=variables_id_agrupacion,
            columns=['característica', 'observación'],
            values='conteo',
            fill_value=0
        )
        
        columnas_conteos = [col for col in df_agregado.columns if col not in variables_id_agrupacion]
        for col in columnas_conteos:
            df_agregado[col] = df_agregado[col].astype(int)
        df_agregado.columns = [f'{columna}-{valor}' for columna, valor in df_agregado.columns]
        df_agregado = df_agregado.reset_index()
        
        return df_agregado
    
    
    def agrupar_variables_numericas(self, variables_id_agrupacion:list, variables_a_agrupar:list, operacion:str):
        
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El valor del parámetro variables_id_agrupacion debe ser de tipo list')
        
        if not isinstance(variables_a_agrupar, list):
            raise TypeError('El valor del parámetro variables_a_agrupar debe ser de tipo list')
        
        df = self.df[variables_id_agrupacion + variables_a_agrupar]
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
        
        variables_numericas = []
        for var in variables_a_agrupar:
            try:
                df[var] = pd.to_numeric(df[var], errors='raise')
                variables_numericas.append(var)
            except Exception:
                print(f'La variable {var} contiene valores no numéricos (o no convertibles a numérico), no se agrupará')
        df = df[variables_id_agrupacion + variables_numericas]
        df = df.loc[:, ~df.columns.duplicated()] # si las variables de agrupacion se encuentran repetidas en las variables a agrupar, se eliminan

        df_agregado = pd.DataFrame()
        if operacion == 'suma':
            df_agregado = df.groupby(variables_id_agrupacion, as_index=False).sum()
        elif operacion == 'media': 
            df_agregado = df.groupby(variables_id_agrupacion, as_index=False).mean()
        elif operacion == 'mediana':
            df_agregado = df.groupby(variables_id_agrupacion, as_index=False).median()
        else:
            raise ValueError('La operación especificada no existe, se implementan las siguientes: suma, media, mediana')
        
        df_agregado = df_agregado.copy() # defragmentacion
        
        renombramiento_columnas = [
            var if var in variables_id_agrupacion else f"{operacion}::{var}"
            for var in df_agregado.columns
        ]
        df_agregado.columns = renombramiento_columnas        
        
        return df_agregado
    

    def agrupar_total_datos(self, variables_id_agrupacion):
        
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El parámetro variables_id_agrupacion debe ser de tipo list')
        
        return self.df.groupby(variables_id_agrupacion).size().reset_index(name='conteo::total_datos')
    
