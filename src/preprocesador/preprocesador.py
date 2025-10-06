import pandas as pd
import ast

class Preprocesador:
    """
    Clase para preprocesar y transformar datos en un DataFrame de pandas,
    utilizando metadatos para definir operaciones como renombrado, exclusión,
    conversión de tipos y agrupaciones, así como la generación de archivos de 
    traducción a alias para la agrupación de dichas variables.
    """
    def __init__(self, df:pd.DataFrame, metadatos:pd.DataFrame, columna_metadatos_nombres:str, columna_metadatos_posibles_valores:str):
        """
        Inicializa el preprocesador con los datos y metadatos.

        Args:
            df (pd.DataFrame): DataFrame principal con los datos.
            metadatos (pd.DataFrame): DataFrame con los metadatos de las variables en el DataFrame.
            columna_metadatos_nombres (str): Nombre de la columna en los metadatos con los nombres de variables presentes en el DataFrame.
            columna_metadatos_posibles_valores (str): Nombre de la columna en los metadatos con los posibles valores de las variables en el DataFrame.
        
        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
        """
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
        
    def eliminar_cadenas_vacias(self) -> None:
        """
        Reemplaza cadenas vacías o espacios en blanco en el DataFrame por valores NA de pandas.
        """
        self.df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        
    def columnas_a_alias(self, columna_metadatos_alias:str) -> None:
        """
        Renombra las columnas del DataFrame dada una columna en los metadatos con alias para las variables.

        Args:
            columna_metadatos_alias (str): Nombre de la columna de metadatos con los alias de variables.
        
        Raises:
            TypeError: Si el parámetro no es del tipo esperado.
        """
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        
        diccionario_variables = dict(zip(self.metadatos[self.columna_metadatos_nombres], self.metadatos[columna_metadatos_alias]))
        self.df.rename(columns=diccionario_variables, inplace=True)
        
    def convertir_tipos(self, columna_metadatos_tipos:str) -> None:
        """
        Convierte los tipos de las columnas del DataFrame según lo especificado en la columna especificada de los metadatos.

        Args:
            columna_metadatos_tipos (str): Nombre de la columna en los metadatos con los tipos de dato a convertir.
        
        Raises:
            TypeError: Si el parámetro no es del tipo esperado.
            KeyError: Si alguna variable no se encuentra en el DataFrame.
        """
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

    def excluir_variables(self, columna_metadatos_filtro_excluir:str, valores_a_excluir:list) -> None:
        """
        Excluye variables del DataFrame según su valor asignado en la columna especificada de los metadatos.
        
        Args:
            columna_metadatos_filtro_excluir (str): Nombre de la columna de metadatos para filtrar variables.
            valores_a_excluir (list): Lista de valores a excluir.
            
        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
        """
        if not isinstance(columna_metadatos_filtro_excluir, str):
            raise TypeError('El parámetro columna_metadatos_filtro_excluir debe ser de tipo str')
        if not isinstance(valores_a_excluir, list):
            raise TypeError('El parámetro valores_a_excluir debe ser de tipo list')

        metadatos_filtrados = self.metadatos.loc[~self.metadatos[columna_metadatos_filtro_excluir].isin(valores_a_excluir)]
        columnas_filtradas = [col for col in metadatos_filtrados[self.columna_metadatos_nombres] if col in self.df.columns]

        self.df = self.df[columnas_filtradas]
        
    def generar_diccionario_traducciones_variables_categoricas(self, variables:list, columna_metadatos_alias:str, columna_metadatos_posibles_valores_alias:str) -> dict:
        """
        Genera un diccionario de traducción para variables categóricas, mapeando nombre_original-_valor_original a sus alias definidos en las columnas especificadas de los metadatos
        de forma nombre_alias-posible_valor_alias.
        Nota: Si se utiliza previamente la función columna_a_alias, la columna de alias especificada ahí se convertirá en la nueva columna de nombres de variables, por lo que si
        se vuelve a utilizar esa misma columna en esta función, se generará un diccionario con nombres y traducciones iguales.

        Args:
            variables (list): Lista de variables categóricas que se incluirán en el diccionario (se generará una traducción para cada combinación nombre-posible_valor).
            columna_metadatos_alias (str): Columna de metadatos con los alias de variables.
            columna_metadatos_posibles_valores_alias (str): Columna de metadatos con los alias de los posibles valores.

        Returns:
            dict: Diccionario de traducción.
            
        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los valores de los metadatos no pueden ser evaluados como listas.
        """
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
    
    
    def generar_diccionario_traducciones_variables_numericas(self, variables:list, columna_metadatos_alias:str, operacion:str) -> dict:
        """
        Genera un diccionario de traducción para variables numéricas, mapeando operacion_aplicada::nombre_original a sus alias definidos en las columna especificada de los metadatos
        de forma operacion_aplicada::nombre_alias.
        Nota: Si se utiliza previamente la función columna_a_alias, la columna de alias especificada ahí se convertirá en la nueva columna de nombres de variables, por lo que si
        se vuelve a utilizar esa misma columna en esta función, se generará un diccionario con nombres y traducciones iguales.

        Args:
            variables (list): Lista de variables numéricas que se incluirán en el diccionario (se generará una traducción para cada variable con la operación especificada).
            columna_metadatos_alias (str): Columna de metadatos con los alias de variables.
            operacion (str): Operación a incluir en la traducción (ej. 'suma', 'media').

        Returns:
            dict: Diccionario de traducción.
            
        Raises:
            TypeError: Si los argumentos no son del tipo esperado.
        """
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
    
    
    def generar_diccionario_total_datos(self) -> dict:
        """
        Genera un diccionario con la traducción para la columna que representa el conteo total de datos, el nombre de esta es constante: conteo::total_datos.

        Returns:
            dict: Diccionario de traducción para la columna que representa el conteo total de datos.
        """
        return {'conteo::total_datos':'conteo::total_datos'}


    def agrupar_variables_categoricas(self, variables_id_agrupacion, variables_a_agrupar):
        """
        Agrupa y cuenta las combinaciones de variable-posible_valor por identificadores de agrupación.

        Args:
            variables_id_agrupacion (list): Variables que identidican los grupos a generar.
            variables_a_agrupar (list): Variables categóricas a agrupar, se genera una columna para el conteo de cada posible valor que toma cada variable en los grupos.

        Returns:
            pd.DataFrame: DataFrame con datos agregados.
            
        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
        """
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
    
    
    def agrupar_variables_numericas(self, variables_id_agrupacion:list, variables_a_agrupar:list, operacion:str) -> pd.DataFrame:
        """
        Agrupa y aplica una operación (suma, media, mediana) sobre variables numéricas.

        Args:
            variables_id_agrupacion (list): Variables que identidican los grupos a generar.
            variables_a_agrupar (list): Variables numéricas a agrupar, se genera una columna por cada variable, transformada por la operación a aplicar en cada grupo.
            operacion (str): Operación a aplicar ('suma', 'media', 'mediana').

        Returns:
            pd.DataFrame: DataFrame con los datos agregados.
            
        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si la operación especificada no es válida.
        """
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
    

    def agrupar_total_datos(self, variables_id_agrupacion:list) -> pd.DataFrame:
        """
        Cuenta el total de datos por cada grupo generado.

        Args:
            variables_id_agrupacion (list): Variables que identidican los grupos a generar.

        Raises:
            TypeError: Si el parámetro no es del tipo esperado.
        """
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El parámetro variables_id_agrupacion debe ser de tipo list')
        
        return self.df.groupby(variables_id_agrupacion).size().reset_index(name='conteo::total_datos')
    
