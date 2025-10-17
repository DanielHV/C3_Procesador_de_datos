# C3_Procesador_de_datos

Procesamiento de fuentes de datos heterogéneas de interés en el C3.

---

## Estructura del repositorio

```
C3_Procesador_de_datos/
├── data/                  # Datos de entrada y salida (raw, preprocessed, processed)
├── notebooks/             # Notebooks de generación de diccionarios de datos
├── src/
│   ├── procesador/        # Clasificación y procesamiento de variables
│   │   ├── procesador.py
│   │   └── main_procesador.py
│   ├── preprocesador/     # Preprocesamiento y agrupación de datos
│   │   ├── preprocesador.py
│   │   └── main_preprocesador.py
│   └── conexion_base/     # Carga a base de datos
│       └── conexion_base_datos.py
├── .gitignore
├── LICENSE
└── README.md
```

---

## Dependencias

Instalación:

```sh
pip install -r requirements.txt
```

---

## Flujo general

1. **Preprocesamiento de datos** ([`src/preprocesador/main_preprocesador.py`](src/preprocesador/preprocesador.py)): Limpiar, transformar y agrupar los datos originales, generando archivos preprocesados y diccionarios de alias.

2. **Categorización y procesamiento** ([`src/procesador/main_procesador.py`](src/procesador/procesador.py)): Normalizar variables categorizarlas en cuantiles, y procesar el resultado generando archivos estructurados para análisis subsecuente.

3. **Carga a base de datos** ([`src/conexion_base/conexion_base_datos.py`](src/conexion_base/conexion_base_datos.py)): Cargar los archivos procesados a una base de datos PostgreSQL.

---

## Preprocesamiento y agrupación de datos

### Ejecución

```sh
cd src
python -m preprocesador.main_preprocesador --config preprocesador/preprocesador_example.json
```

- El archivo de configuración define rutas de entrada/salida, columnas de diccionario de datos, variables a agrupar, etc.

### Características de entrada

En este paso se realiza un preprocesamiento y agrupación de los datos provenientes de su fuente original, sin embargo, se espera que los archivos cumplan con ciertas características mínimas para el correcto funcionamiento del programa, a continuación se describen los dos archivos mínimos, así como las columnas que se pueden reconocer en estos:
1. Archivo de datos
  - Obligatorio
    - Columna que tenga como valores identificadores utilizados para crear agrupaciones en los datos (por ejemplo, identificador para agrupar datos en municipio, estado, etc.).
2. Archivo de diccionario de datos que describe todas las variables en el archivo de datos
  - Obligatorio
    - Columna que tenga como valores los nombres de las variables tal cual se escriben en las columnas del archivo de datos.
    - Columna que tenga como valores listas (cadenas que representan listas de Python), que consisten de todos los posibles valores que toma la variable en el archivo de datos. En caso de tratarse de una variable no categórica, se tiene una lista vacía.
  - Opcional (no indispensable, pero pueden ser utilizadas para transformar los datos además de agruparlos)
    - Columna que tenga como valores nombres alternativos (alias) para los nombres de las variables.
    - Columna que tenga como valores listas (cadenas que representan listas de Python), que consisten de nombres alternativos (alias) para todos los posibles valores que toma la variable en el archivo de datos. En caso de tratarse de una variable no categórica, se tiene una lista vacía. (Nota: para el valor en la posición i de la lista de posibles valores, su alias será el valor en la posición i de la lista de alias de posibles valores).
    - Columna que tenga como valores etiquetas (cadenas) con el propósito de filtrar solo variables específicas al momento de agrupar.

### Archivo de configuración 

Ejemplo: `config/preprocesador_example_ensanut.json`

```json
{
    "ruta_csv_diccionario_datos" : "../data/ensanut/raw/21ensanut_a20_hias_rework.csv",
    "ruta_csv_dataset" : "../data/ensanut/raw/salud_hogar_orig_lower_promedios_for_elastic.csv",
    "ruta_salida_dataset" : "../data/ensanut/preprocessed/preprocesamiento_ensanut_example_mun.csv",
    "ruta_salida_alias" : "../data/ensanut/preprocessed/preprocesamiento_ensanut_alias_example_mun.csv",

    "columna_diccionario_nombres" : "var",
    "columna_diccionario_posibles_valores" : "posibles_valores_alias",

    "columna_diccionario_alias" : "var_alias",
    "columna_diccionario_posibles_valores_alias" : "posibles_valores",

    "variables_identificadoras_list" : ["municipio"],
    "variables_a_agrupar" : [
        {
            "tipo_variables" : "categorico",
            "variables_a_agrupar_list" : [],
            "variables_a_agrupar_regex" : [],
            "variables_a_agrupar_clasificacion_diccionario" : {
                "columna_diccionario_filtro" : "var_type",
                "valores": ["options"]
            }
        },
        {
            "tipo_variables" : "numerico",
            "operacion" : "media",
            "variables_a_agrupar_list" : [],
            "variables_a_agrupar_regex" : [],
            "variables_a_agrupar_clasificacion_diccionario" : {
                "columna_diccionario_filtro" : "var_type",
                "valores": ["abierta"]
            }
        }
    ]
}
```

- `"ruta_csv_diccionario_datos"`:  
  Ruta al archivo de diccionario de datos que describe las variables del dataset, sus tipos, valores posibles, alias, etc. Es necesario para la selección automática de variables y para la generación de diccionarios de alias.

- `"ruta_csv_dataset"`:  
  Ruta al archivo de datos.

- `"ruta_salida_dataset"`:  
  Ruta donde se guardará el archivo de datos preprocesados y transformados, resultado de todas las operaciones y agrupaciones configuradas.

- `"ruta_salida_alias"`:  
  Ruta donde se guardará el dataframe de alias, que mapea nombres originales a alias o códigos.

- `"columna_diccionario_nombres"` y `"columna_diccionario_posibles_valores"`:  
  Nombres de las columnas en el archivo de diccionario de datos que contienen los nombres originales de las variables y sus valores posibles, respectivamente. Estas columnas son obligatorias.

- `"columna_diccionario_alias"` y `"columna_diccionario_posibles_valores_alias"`:  
  Nombres de las columnas en el archivo de diccionario de datos que contienen los alias de las variables y los alias de sus valores posibles, que serán utilizados para dar alias a los nombres originales en el diccionario. Estos campos son obligatorios, sin embargo, es posible establecer las mismas columnas con nombres originales como columnas de alias.

- `"variables_identificadoras_list"`:  
  Lista de columnas que identifican de manera única cada grupo sobre el que se realizarán las agregaciones y transformaciones (por ejemplo, `"municipio"`). El resultado tendrá una fila por cada combinación única de estos identificadores.

- `"variables_a_agrupar"`:
  Permite definir distintos tipos de agrupaciones sobre los datos originales, especificando para cada tipo de variable (por ejemplo, categórica o numérica) cómo se deben agrupar y resumir los datos. Cada elemento de la lista `"variables_a_agrupar"` es un diccionario que define:

    - `"tipo_variables"`: El tipo de variable a agrupar (`"categorico"` para variables de opciones, `"numerico"` para variables continuas).

        - **Variables categóricas**:  Se cuenta, para cada grupo (por ejemplo, municipio), cuántos registros hay de cada valor posible de cada variable categórica seleccionada. El resultado tendrá columnas con el formato `variable-valor`, representando el conteo de cada posible valor de cada variable.
        - **Variables numéricas**:  Se calcula, para cada grupo, el resultado de aplicar la operación especificada (por ejemplo, `"media"`) a cada variable numérica seleccionada. El resultado tendrá columnas con el formato `media::variable`, representando el cálculo de la operación especificada aplicada a cada variable.

    - `"operacion"`: (Solo para variables numéricas) La operación de agregación a aplicar, por ejemplo `"media"` para promedio.
    - `"variables_a_agrupar_list"`: Lista explícita de variables a agrupar.
    - `"variables_a_agrupar_regex"`: Lista de expresiones regulares para seleccionar variables a agrupar.
    - `"variables_a_agrupar_clasificacion_diccionario"`: (Opcional) Permite seleccionar variables automáticamente según una columna del diccionario de datos (por ejemplo, todas las variables cuyo valor en la columna `"var_type"` sea `"options"` o `"abierta"`).

### Salida

El archivo de salida contendrá, para cada grupo, los conteos de cada valor posible de las variables categóricas, el resultado de aplicar la operación especificada a las variables numéricas, y el total de registros que se incluye automáticamente en la columna `conteo::total_datos`.
Además, se generará un archivo de diccionario de alias, necesario en el paso de procesamiento.

---

## Clasificación y procesamiento

### Ejecución

```sh
cd src
python -m procesador.main_procesador --config procesador/procesador_example.json
```

- El archivo de configuración define rutas de entrada, variables a procesar, exclusiones, número de cuantiles, etc.

### Características de entrada

En este paso se realiza una clasificación en cuantiles y procesamiento de los datos previamente preprocesados y agrupados, se espera que los archivos cumplan con ciertas características mínimas para el correcto funcionamiento del programa, a continuación se describen los dos archivos mínimos, así como las columnas que se pueden reconocer en estos:
1. Archivo de datos
  - Obligatorio
    - Columna que tenga como valores identificadores para cada grupo existente (por ejemplo, municipios, estados, etc.).
2. Archivo de alias
  - Obligatorio
    - Columna que tenga como valores los nombres de las variables tal cual se escriben en las columnas del archivo de datos. Nota: El programa solo procesará variables existentes en esta columna, si se especifica una variable que no existe, se omitirá.
    - Columna que tenga como valores los nombres alternativos (alias) de las variables.

### Archivo de configuración

Ejemplo: `config/procesador_example_ensanut.json`

```json
{
    "rutas_csv_mallas" : {
        "mun" : "../data/ensanut/preprocessed/preprocesamiento_ensanut_example_mun.csv"
    },
    "ruta_csv_dataframe_alias" : "../data/ensanut/preprocessed/preprocesamiento_ensanut_alias_example_mun.csv",
    "ruta_csv_salida" : "../data/ensanut/processed/procesamiento_ensanut_example.csv",

    "columna_dataframe_alias_nombres" : "alias",
    "columna_dataframe_alias_alias" : "variable",

    "variables_identificadoras" : ["municipio"],

    "variables_excluidas_list" : ["agua_lavar_ropa-88", "agua_lavar_ropa-99"],
    "variables_excluidas_regex" : ["^entidad.*", "^region.*"],

    "variables_a_procesar_list" : {
        "None" : ["conteo::total_datos"],
        "conteo::total_datos" : ["salud_tiene_obesidad-1", "salud_tiene_obesidad-2"]
    },
    "variables_a_procesar_regex" : {
        "None" : ["^agua.*"],
        "conteo::total_datos" : ["^accidente.*", "^secuelas_post_covid.*"]
    },

     "q" : 10
}
```

- `"rutas_csv_mallas"`:  
  Diccionario que indica las rutas a los archivos CSV de entrada para cada malla diferente (por ejemplo, `"mun"` para municipio). Cada clave es el nombre de la malla y el valor es la ruta al archivo correspondiente.

- `"ruta_csv_dataframe_alias"`:  
  Ruta al archivo CSV que contiene el dataframe de alias de variables y valores, generado en el preprocesamiento.

- `"ruta_csv_salida"`:  
  Ruta donde se guardará el archivo CSV con los resultados del procesamiento.

- `"columna_dataframe_alias_nombres"`:  
  Nombre de la columna en el dataframe de alias que contiene los nombres descriptivos de las variables.

- `"columna_dataframe_alias_alias"`:  
  Nombre de la columna en el dataframe de alias que contiene los alias o nombres cortos de las variables.

- `"variables_identificadoras"`:  
  Lista de columnas que identifican de manera única cada grupo (entidad) en  (por ejemplo, `"municipio"`).

- `"variables_excluidas_list"`:  
  Lista explícita de variables (o columnas) que se deben excluir del procesamiento.

- `"variables_excluidas_regex"`:  
  Lista de expresiones regulares para excluir variables cuyo nombre coincida con alguno de los patrones especificados.

- `"variables_a_procesar_list"`:  
  Diccionario que indica pares de: variable utilizada como base de normalización (clave), y una lista de variables a procesar (valor). Si la clave es `"None"`, las variables se procesan sin normalizar.

    - Ejemplo:  
      ```json
      {
        "None": ["conteo::total_datos"],
        "conteo::total_datos": ["salud_tiene_obesidad-1", "salud_tiene_obesidad-2"]
      }
      ```
      Esto procesará `"conteo::total_datos"` sin normalizar y `"salud_tiene_obesidad-1"` y `"salud_tiene_obesidad-2"` normalizadas por `"conteo::total_datos"`.

- `"variables_a_procesar_regex"`:  
  Diccionario que indica pares de: variable utilizada como base de normalización (clave), y una lista de expresiones regulares para seleccionar variables a procesar (valor). Si la clave es `"None"`, las variables se procesan sin normalizar.

    - Ejemplo:  
      ```json
      {
        "None": ["^agua.*"],
        "conteo::total_datos": ["^accidente.*", "^secuelas_post_covid.*"]
      }
      ```
      Esto selecciona todas las variables que empiezan con `"agua"` para procesar sin normalizar, y todas las que empiezan con `"accidente"` o `"secuelas_post_covid"` para procesar normalizadas por `"conteo::total_datos"`.

- `"q"`:  
  Número de categorías (cuantiles) en las que se dividirán las variables durante la categorización.

### Salida

El archivo de salida generado por el procesamiento es un archivo CSV en formato largo, donde cada fila representa una categoría (bin) de una variable procesada para una malla específica (el número de filas por variable será igual al número de categorías (`q`) más una posible fila adicional para "Sin clasificar"). La estructura general incluye las siguientes columnas:

- **name**: Nombre descriptivo de la variable procesada (según el diccionario de alias).
- **code**: Alias o código de la variable procesada.
- **bin**: Número de la categoría o bin asignado (por ejemplo, 1 a q, donde q es el número de cuantiles).
- **interval_{malla}**: Intervalo numérico o de porcentaje correspondiente a la categoría para cada malla (por ejemplo, `interval_mun`).
- **cells_{malla}**: Conjunto de entidades (por ejemplo, municipios) que pertenecen a ese intervalo/categoría para la malla correspondiente.

---

## Carga a base de datos

### Ejecución

```sh
cd src
python conexion_base/conexion_base_datos.py --ruta-datos-procesados ../data/covid19/processed/procesamiento_covid19_example.csv --ruta-env ./.env --crear-tabla
```

- `--ruta-datos-procesados`: Ruta del archivo generado en el paso de procesamiento.
- `--ruta-env`: Ruta del archivo de configuración, por defecto `.`
- `--crear-tabla`: No toma ningún valor, si se incluye, se crea la tabla especificada en caso de no existir en la base de datos.

### Archivo de configuración

- El archivo `.env` debe contener las credenciales y parámetros de la base de datos:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_de_base
DB_USER=usuario
DB_PASSWORD=contrasena
DB_TABLE=nombre_de_tabla
```

---

## Notebooks

En la carpeta [`notebooks/`](notebooks/) se incluyen los procedimientos de generación de archivos de diccionarios de datos compatibles con el paso de preprocesamiento para las fuentes de datos de ejemplo.

---
