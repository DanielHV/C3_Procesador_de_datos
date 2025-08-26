import pandas as pd
import re
import ast

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

class Preprocesador:
    
    def __init__(self, df:pd.DataFrame, metadatos:pd.DataFrame):

        if not isinstance(df, pd.DataFrame):
            raise TypeError('El valor del parámetro df debe ser de tipo pd.DataFrame')
        self.df = df
        self.metadatos = metadatos
        
    def eliminar_cadenas_vacias(self):
        
        self.df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        
    def nombres_a_alias(self, columna_metadatos_nombres:str, columna_metadatos_alias:str):
        
        if not isinstance(columna_metadatos_nombres, str):
            raise TypeError('El parámetro columna_metadatos_nombres debe ser de tipo str')
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        
        diccionario_variables = dict(zip(self.metadatos[columna_metadatos_nombres], self.metadatos[columna_metadatos_alias]))
        self.df.rename(columns=diccionario_variables, inplace=True)
        
    def convertir_tipos(self, columna_metadatos_nombres:str, columna_metadatos_tipos:str):
        
        if not isinstance(columna_metadatos_nombres, str):
            raise TypeError('El parámetro columna_metadatos_nombres debe ser de tipo str')
        if not isinstance(columna_metadatos_tipos, str):
            raise TypeError('El parámetro columna_metadatos_tipos debe ser de tipo str')
        
        try: 
            for var, tipo in zip(self.metadatos[columna_metadatos_nombres], self.metadatos[columna_metadatos_tipos]):
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

    def excluir_variables(self, columna_metadatos_nombres:str, columna_metadatos_filtro_excluir:str, valores_a_excluir:list):

        if not isinstance(columna_metadatos_nombres, str):
            raise TypeError('El parámetro columna_metadatos_nombres debe ser de tipo str')
        if not isinstance(columna_metadatos_filtro_excluir, str):
            raise TypeError('El parámetro columna_metadatos_filtro_excluir debe ser de tipo str')
        if not isinstance(valores_a_excluir, list):
            raise TypeError('El parámetro valores_a_excluir debe ser de tipo list')

        metadatos_filtrados = self.metadatos.loc[~self.metadatos[columna_metadatos_filtro_excluir].isin(valores_a_excluir)]

        self.df = self.df[metadatos_filtrados[columna_metadatos_nombres]]
        
    def generar_metadatos_agregados_observacion(self, columna_metadatos_nombres:str, columna_metadatos_alias:str, columna_metadatos_posibles_valores:str, columna_metadatos_posibles_valores_alias:str):

        if not isinstance(columna_metadatos_nombres, str):
            raise TypeError('El parámetro columna_metadatos_nombres debe ser de tipo str')
        if not isinstance(columna_metadatos_alias, str):
            raise TypeError('El parámetro columna_metadatos_alias debe ser de tipo str')
        if not isinstance(columna_metadatos_posibles_valores, str):
            raise TypeError('El parámetro columna_metadatos_posibles_valores debe ser de tipo str')
        if not isinstance(columna_metadatos_posibles_valores_alias, str):
            raise TypeError('El parámetro columna_metadatos_posibles_valores_alias debe ser de tipo str')

        columna_var_posibles_valores = []
        columnas_var_posibles_valores_alias = []
        for var, posibles_valores_list, var_alias, posibles_valores_alias_list in zip(
                self.metadatos[columna_metadatos_nombres],
                self.metadatos[columna_metadatos_posibles_valores],
                self.metadatos[columna_metadatos_alias],
                self.metadatos[columna_metadatos_posibles_valores_alias]):
            posibles_valores = ast.literal_eval(posibles_valores_list)
            posibles_valores_alias = ast.literal_eval(posibles_valores_alias_list)
            lista_combinaciones_nombres = []
            lista_combinaciones_alias = []
            for valor, valor_alias in zip(posibles_valores, posibles_valores_alias):
                lista_combinaciones_nombres.append(f'{var}-{valor}')
                lista_combinaciones_alias.append(f'{var_alias}-{valor_alias}')
            columna_var_posibles_valores.append(lista_combinaciones_nombres)
            columnas_var_posibles_valores_alias.append(lista_combinaciones_alias)

        self.metadatos[f'{columna_metadatos_nombres}-{columna_metadatos_posibles_valores}'] = columna_var_posibles_valores
        self.metadatos[f'{columna_metadatos_alias}-{columna_metadatos_posibles_valores_alias}'] = columnas_var_posibles_valores_alias
                 
    def generar_datos_agregados(self, variables_id_agrupacion, variables_a_agrupar, tipo_valores):
        
        # validar tipos
        # validar variables en dataset
        
        # normalizar texto de tipo_valores
        if tipo_valores == 'observacion':

            df = self.df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
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
            
            df_agregado = df_agregado.astype(int)
            df_agregado.columns = [f'{columna}-{valor}' for columna, valor in df_agregado.columns]
            df_agregado = df_agregado.reset_index()
            
            return df_agregado
            
        elif tipo_valores == 'conteo':

            df = self.df.copy()
            try:
                for var in variables_a_agrupar:
                    conversion = pd.to_numeric(df[var], erros='raise')
            except Exception as e:
                raise ValueError('Error al convertir la variable {var} a tipo numérico\n{e}')

            df_melt = df.melt(
                id_vars=variables_id_agrupacion,
                value_vars=variables_a_agrupar,
                var_name='característica-observación',
                value_name='conteo'
            )
            
            df_suma_conteos = (
                df_melt
                .groupby(variables_id_agrupacion+['característica-observación'], as_index=False)
                .sum()
            )
            
            df_agregado = df_suma_conteos.pivot_table(
                index=variables_id_agrupacion,
                columns='característica-observación',
                values='conteo'
            )
            
            df_agregado.columns.name = None
            df_agregado = df_agregado.reset_index()
            
            return df_agregado
            
        else:
            
            ValueError('El valor del parámetro tipo_valores debe ser observacion o conteo')
