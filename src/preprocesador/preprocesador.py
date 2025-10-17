import pandas as pd
import ast

class Preprocesador:
    """
    Clase para preprocesar y transformar datos en un DataFrame de pandas,
    utilizando metadatos para definir operaciones como renombrado, exclusión,
    conversión de tipos y agrupaciones, así como la generación de archivos de alias
    para la agrupación de dichas variables.
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
        # validaciones df
        if not isinstance(df, pd.DataFrame):
            raise TypeError('El valor del parámetro df debe ser de tipo pd.DataFrame')
        
        # validaciones metadatos
        if not isinstance(metadatos, pd.DataFrame):
            raise TypeError('El valor del parámetro metadatos debe ser de tipo pd.DataFrame')
        
        # validaciones columna_metadatos_nombres
        if not isinstance(columna_metadatos_nombres, str):
            raise TypeError('El parámetro columna_metadatos_nombres debe ser de tipo str')
        
        # validaciones columa_metadatos_posibles_valores
        if not isinstance(columna_metadatos_posibles_valores, str):
            raise TypeError('El parámetro columna_metadatos_posibles_valores debe ser de tipo str')
        
        # asignacion de atributos
        self.df = df
        self.metadatos = metadatos
        self.columna_metadatos_nombres = columna_metadatos_nombres
        self.columna_metadatos_posibles_valores = columna_metadatos_posibles_valores
        
        # eliminar columna 'Unnamed: 0' (esta existe cuando se tiene una primera columna de indices sin nombre)
        if 'Unnamed: 0' in self.df.columns:
            self.df.drop(columns=['Unnamed: 0'], inplace=True)
        if 'Unnamed: 0' in self.metadatos.columns:
            self.metadatos.drop(columns=['Unnamed: 0'], inplace=True)
            
    def eliminar_cadenas_vacias(self) -> None:
        """
        Reemplaza cadenas vacías o espacios en blanco en el DataFrame por valores NA de pandas.
        """
        # la funcion replace toma como primer parametro una expresion regular que acepta las cadenas vacias a sustituir,
        # y como segundo parametro pd.NA que sera el valor que sustituye
        self.df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        
    def columnas_a_alias(self, columna_metadatos_alias:str) -> None:
        """
        Renombra las columnas del DataFrame dada una columna en los metadatos con alias para las variables.

        Args:
            columna_metadatos_alias (str): Nombre de la columna de metadatos con los alias de variables.
        
        Raises:
            TypeError: Si el parámetro no es del tipo esperado.
        """
        # validaciones columna_metadatos_alias
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        
        # crear un diccionario de mapeo entre nombres originales y alias
        diccionario_variables = dict(zip(self.metadatos[self.columna_metadatos_nombres], self.metadatos[columna_metadatos_alias]))
        # renombrar columnas del dataframe usando el diccionario
        self.df.rename(columns=diccionario_variables, inplace=True)
         # actualizar el atributo correspondiente a la columna que contiene los nombres de las variables en el dataframe
        self.columna_metadatos_nombres = columna_metadatos_alias
        
    def convertir_tipos(self, columna_metadatos_tipos:str) -> None:
        """
        Convierte los tipos de las columnas del DataFrame según lo especificado en la columna especificada de los metadatos.

        Args:
            columna_metadatos_tipos (str): Nombre de la columna en los metadatos con los tipos de dato a convertir.
        
        Raises:
            TypeError: Si el parámetro no es del tipo esperado.
            KeyError: Si alguna variable no se encuentra en el DataFrame.
        """
        # validaciones columna_metadatos_tipos
        if not isinstance(columna_metadatos_tipos, str):
            raise TypeError('El parámetro columna_metadatos_tipos debe ser de tipo str')
        
        # para cada entrada en los metadatos se obtiene el valor en la columna de nombres y el valor en la columna de tipos
        for var, tipo in zip(self.metadatos[self.columna_metadatos_nombres], self.metadatos[columna_metadatos_tipos]):
            try: 
                # convertir el tipo de cada columna según lo especificado en la columna de tipos 
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
        # validaciones columna_metadatos_filtro_excluir
        if not isinstance(columna_metadatos_filtro_excluir, str):
            raise TypeError('El parámetro columna_metadatos_filtro_excluir debe ser de tipo str')
        
        # validaciones valores_a_excluir
        if not isinstance(valores_a_excluir, list):
            raise TypeError('El parámetro valores_a_excluir debe ser de tipo list')

        # filtrar metadatos para excluir las variables especificadas
        metadatos_filtrados = self.metadatos.loc[~self.metadatos[columna_metadatos_filtro_excluir].isin(valores_a_excluir)]
        # obtener las columnas filtradas que existen en el dataframe
        columnas_filtradas = [col for col in metadatos_filtrados[self.columna_metadatos_nombres] if col in self.df.columns]
        # actualizar el dataframe manteniendo solo las columnas filtradas
        self.df = self.df[columnas_filtradas]
        
    def generar_alias_variables_categoricas(self, variables:list, columna_metadatos_alias:str, columna_metadatos_posibles_valores_alias:str) -> dict:
        """
        Genera un diccionario de alias para variables categóricas, mapeando nombre_original-_valor_original a sus alias definidos en las columnas especificadas de los metadatos
        de forma nombre_alias-posible_valor_alias.
        Nota: Si se utiliza previamente la función columna_a_alias, la columna de alias especificada ahí se convertirá en la nueva columna de nombres de variables, por lo que si
        se vuelve a utilizar esa misma columna en esta función, se generará un diccionario con nombres y alias iguales.

        Args:
            variables (list): Lista explícita de variables categóricas que se incluirán en el diccionario (se generará un alias para cada combinación nombre-posible_valor).
            columna_metadatos_alias (str): Columna de metadatos con los alias de variables.
            columna_metadatos_posibles_valores_alias (str): Columna de metadatos con los alias de los posibles valores.

        Returns:
            dict: Diccionario de alias.
            
        Raises:
            TypeError: Si los parámetros no son del tipo esperado.
            ValueError: Si los valores de los metadatos no pueden ser evaluados como listas.
        """
        # validaciones variables
        if not isinstance(variables, list):
            raise TypeError('El parámetro variables debe ser de tipo list')
        
        # validaciones columna_metadatos_alias
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        
        # validaciones columna_metadatos_posibles_valores_alias
        if not isinstance(columna_metadatos_posibles_valores_alias, str):
            raise TypeError('El parámetro columna_metadatos_posibles_valores_alias debe ser de tipo str')
        
        # inicializar un diccionario vacio para almacenar alias
        alias = {}
        
        for variable in variables:
            # obtener la fila de metadatos correspondiente a la variable
            fila = self.metadatos[self.metadatos[self.columna_metadatos_nombres] == variable]
            if fila.empty:
                continue
            # obtener el alias de la variable desde los metadatos
            variable_alias = fila[columna_metadatos_alias].values[0]
            # obtener los posibles valores y sus alias como cadenas, se evaluan para construir listas
            posibles_valores = ast.literal_eval(fila[self.columna_metadatos_posibles_valores].values[0])
            posibles_valores_alias = ast.literal_eval(fila[columna_metadatos_posibles_valores_alias].values[0])
            
            # generar alias para cada combinación de valor y alias
            for valor, valor_alias in zip(posibles_valores, posibles_valores_alias):
                alias[f'{variable}-{valor}'] = f'{variable_alias}-{valor_alias}'

        return alias
    
    
    def generar_alias_variables_numericas(self, variables:list, columna_metadatos_alias:str, operacion:str) -> dict:
        """
        Genera un diccionario de alias para variables numéricas, mapeando operacion_aplicada::nombre_original a sus alias definidos en las columna especificada de los metadatos
        de forma operacion_aplicada::nombre_alias.
        Nota: Si se utiliza previamente la función columna_a_alias, la columna de alias especificada ahí se convertirá en la nueva columna de nombres de variables, por lo que si
        se vuelve a utilizar esa misma columna en esta función, se generará un diccionario con nombres y alias iguales.

        Args:
            variables (list): Lista de variables numéricas que se incluirán en el diccionario (se generará un alias para cada variable con la operación especificada).
            columna_metadatos_alias (str): Columna de metadatos con los alias de variables.
            operacion (str): Operación a incluir en el alias (ej. 'suma', 'media').

        Returns:
            dict: Diccionario de alias.
            
        Raises:
            TypeError: Si los argumentos no son del tipo esperado.
        """
        # validaciones variables
        if not isinstance(variables, list):
            raise TypeError('El parámetro variables debe ser de tipo list')
        
        # validaciones columna_metadatos_alias
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        
        # validaciones operaciones
        if not isinstance(operacion, str):
            raise TypeError('El parámetro operacion debe ser de tipo str')
        
        # inicializar un diccionario vacio para almacenar las alias
        alias = {}
        
        for variable in variables:
            # obtener la fila de metadatos correspondiente a la variable
            fila = self.metadatos[self.metadatos[self.columna_metadatos_nombres] == variable]
            if fila.empty:
                continue
            # obtener el alias de la variable desde los metadatos
            variable_alias = fila[columna_metadatos_alias].values[0]
            # generar alias para la variable, representando la aplicación de la operación especificada
            alias[f'{operacion}::{variable}'] = f'{operacion}::{variable_alias}'
        
        return alias
    
    
    def generar_alias_total_datos(self) -> dict:
        """
        Genera un diccionario con un alias fijo para la columna que representa el conteo total de datos: conteo::total_datos.

        Returns:
            dict: Diccionario de alias para la columna que representa el conteo total de datos.
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
        # validaciones variables_id_agrupacion
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El valor del parámetro variables_id_agrupacion debe ser de tipo list')
        
        # validaciones variables_a_agrupar
        if not isinstance(variables_a_agrupar, list):
            raise TypeError('El valor del parámetro variables_a_agrupar debe ser de tipo list')

        # seleccionar las variables de agrupacion y las variables a agrupar
        df = self.df[variables_id_agrupacion+variables_a_agrupar]
        # eliminar espacios en blanco cuando el tipo de dato es str
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
        # eliminar columnas duplicadas en el dataframe
        df = df.loc[:, ~df.columns.duplicated()]

        # transformar el dataframe a formato largo (melt)
        # cada columna original pasa a convertirse en un posible valor de una unica columna 'característica',
        # cada valor en cada columna original pasa a convertir en el valor de una unica columna 'observación'
        df_melt = df.melt(
            id_vars=variables_id_agrupacion,
            value_vars=variables_a_agrupar,
            var_name='característica',
            value_name='observación'
        )
        
        # generar conteos de caracteristica-observacion por grupo
        df_conteos = (
            df_melt
            .groupby(variables_id_agrupacion + ['característica', 'observación'])
            .size()
            .reset_index(name='conteo')
        )
        
        # pivotear el dataframe de conteos para obtener una columna por cada par caracteristica-observacion,
        # donde el valor de cada columna es el conteo del grupo correspondiente
        df_agregado = df_conteos.pivot_table(
            index=variables_id_agrupacion,
            columns=['característica', 'observación'],
            values='conteo',
            fill_value=0
        )
        
        # convertir las columnas a enterios, sin contar las que se usaron como identificadores para agrupar
        columnas_conteos = [col for col in df_agregado.columns if col not in variables_id_agrupacion]
        for col in columnas_conteos:
            df_agregado[col] = df_agregado[col].astype(int)
            
        # renombrar las columnas con el formato 'columna-valor'
        df_agregado.columns = [f'{columna}-{valor}' for columna, valor in df_agregado.columns]
        
        # reiniciar el indice del dataframe
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
        # validaciones variables_id_agrupacion
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El valor del parámetro variables_id_agrupacion debe ser de tipo list')
        
        # validaciones variables_a_agrupar
        if not isinstance(variables_a_agrupar, list):
            raise TypeError('El valor del parámetro variables_a_agrupar debe ser de tipo list')
        
        # seleccionar las variables de agrupacion y las variables a agrupar
        df = self.df[variables_id_agrupacion + variables_a_agrupar]
        # eliminar espacios en blanco cuando el tipo de dato es str
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
        
        # inicializar una lista para almacenar las variables numéricas
        variables_numericas = []
        for var in variables_a_agrupar:
            try:
                # intentar convertir la columna a tipo numerico
                df[var] = pd.to_numeric(df[var], errors='raise')
                variables_numericas.append(var)
            except Exception:
                # si no se puede convertir, se omite la variable
                print(f'La variable {var} contiene valores no numéricos (o no convertibles a numérico), no se agrupará')
                
        # filtrar el dataframe para incluir solo las variables numericas y las de agrupacion
        df = df[variables_id_agrupacion + variables_numericas]
        # eliminar columnas duplicadas en el dataframe
        df = df.loc[:, ~df.columns.duplicated()]

        # inicializar un dataframe vacío para almacenar los resultados
        df_agregado = pd.DataFrame()
        if operacion == 'suma':
            # realizar la suma por cada grupo
            df_agregado = df.groupby(variables_id_agrupacion, as_index=False).sum()
        elif operacion == 'media': 
            # calcular la media por cada grupo
            df_agregado = df.groupby(variables_id_agrupacion, as_index=False).mean()
        elif operacion == 'mediana':
            # calcular la mediana por cada grupo
            df_agregado = df.groupby(variables_id_agrupacion, as_index=False).median()
        else:
            # operacion no valida
            raise ValueError('La operación especificada no existe, se implementan las siguientes: suma, media, mediana')
        
        # defragmentacion
        df_agregado = df_agregado.copy()
        
        # renombrar las columnas para incluir el prefijo de la operación aplicada
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
        # validaciones variables_id_agrupacion
        if not isinstance(variables_id_agrupacion, list):
            raise TypeError('El parámetro variables_id_agrupacion debe ser de tipo list')
        
        # agrupar el dataframe y contar el total de datos en cada grupo
        return self.df.groupby(variables_id_agrupacion).size().reset_index(name='conteo::total_datos')

