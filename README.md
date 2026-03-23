# C3_Procesador_de_datos

Procesamiento de fuentes de datos heterogéneas de interés en el C3.

---

## Estructura del repositorio

```
C3_Procesador_de_datos/
├── data/                  # Datos de entrada y salida (raw, preprocessed, processed)
├── notebooks/             # Notebooks de generación de diccionarios de datos
├── scripts/               # Scripts de transformación de datasets previo al preprocesamiento
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

- El archivo de configuración define rutas de entrada/salida, columnas de diccioario de datos, variables a agrupar, etc.

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
    - Columna que tenga como valores listas (cadenas que representan listas de Python), que consisten de nombres alternativos (alias) para todos los posibles valores que toma la variable en el archivo de datos. En caso de tratarse de uan variable no categórica, se tiene una lista vacía. (Nota: para el valor en la posición i de la lista de posibles valores, su alias será el valor en la posición i de la lista de alias de posibles valores).
    - Columna que tenga como valores etiquetas (cadenas) con el propósito de filtrar solo variables específicas al momento de agrupar.

### Archivo de configuración 

Ejemplo: `config/preprocesador_example_ensanut.json`

```json
```json
{
    "ruta_csv_diccionario_datos" : "../data/covid19/raw/240708 Descriptores_rework.csv",
    "ruta_csv_dataset" : "../data/covid19/raw/COVID19MEXICO_rework.csv",
    "ruta_salida_dataset" : "../data/covid19/preprocessed/preprocesamiento_covid19_example_mun.csv",
    "ruta_salida_alias" : "../data/covid19/preprocessed/preprocesamiento_covid19_alias_example_mun.csv",

    "columna_diccionario_nombres" : "NOMBRE DE VARIABLE",
    "columna_diccionario_posibles_valores" : "POSIBLES VALORES ALIAS",

    "columna_diccionario_alias" : "NOMBRE DE VARIABLE",
    "columna_diccionario_posibles_valores_alias" : "POSIBLES VALORES",
    "columna_diccionario_descripcion" : "DESCRIPCIÓN DE VARIABLE",

    "variables_identificadoras_list_lugares" : ["ENTIDAD_RES", "MUNICIPIO_RES"],
    "variables_a_agrupar_lugares" : [ ... ],

    "tipo_ensamble": "personas",
    "variables_identificadoras_list_personas" : ["ID_REGISTRO"],
    "variables_a_agrupar_personas": [ ... ]
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
  
- `"columna_diccionario_descripcion"` (Opcional):  
  Nombre de la columna en el diccionario de datos que contiene la descripción o información textual de la variable. Estos datos se incluirán como una nueva columna en el archivo alias final.

- `"columna_diccionario_tipos"` (Opcional):  
  Permite realizar conversiones de tipo cast para columnas.

- `"columna_diccionario_filtro_excluir"` y `"valores_a_excluir"` (Opcionales):  
  Permiten establecer una columna del diccionario para filtrar basándose en una lista de valores específicos a excluir del preprocesamiento.

- `"variables_identificadoras_list_lugares"`:  
  Lista de columnas que identifican de manera única cada grupo sobre el que se realizarán las agregaciones y transformaciones orientadas geográficamente o por "lugares" (por ejemplo, `["ENTIDAD_RES", "MUNICIPIO_RES"]`). El resultado tendrá una fila por cada combinación única de estos identificadores.

- `"variables_a_agrupar_lugares"`:
  Permite definir distintos tipos de agrupaciones sobre los datos originales para los lugares identificados, especificando para cada tipo de variable (por ejemplo, categórica o numérica) cómo se deben agrupar y resumir los datos. Cada elemento de la lista es un diccionario que define:

    - `"tipo_variables"`: El tipo de variable a agrupar (`"categorico"` para variables de opciones, `"numerico"` para variables continuas).

        - **Variables categóricas**:  Se cuenta, para cada grupo, cuántos registros hay de cada valor posible de cada variable categórica seleccionada. El resultado tendrá columnas con el formato `variable-valor`.
        - **Variables numéricas**:  Se calcula, para cada grupo, el resultado de aplicar la operación especificada a cada variable numérica seleccionada. El resultado tendrá columnas con el formato `operacion::variable`.

    - `"operacion"`: (Solo para variables numéricas) La operación de agregación a aplicar, por ejemplo `"media"` para promedio o `"mediana"`.
    - `"variables_a_agrupar_list"`: Lista explícita de variables a agrupar.
    - `"variables_a_agrupar_regex"`: Lista de expresiones regulares para seleccionar variables a agrupar.
    - `"variables_a_agrupar_clasificacion_diccionario"`: (Opcional) Permite seleccionar variables automáticamente según una columna del diccionario de datos (por ejemplo, todas las variables cuyo valor en la columna `"var_type"` sea `"options"` o `"abierta"`).

- `"tipo_ensamble"` (Opcional):
  Nombre del tipo de ensamble secundario. Define el sufijo que se usará en los archivos de salida (por ejemplo, `"personas"` genera `_personas.csv`, `"empresas"` genera `_empresas.csv`). Por defecto es `"personas"`. Debe especificarse cuando el dataset no contiene un ensamble estadístico de personas sino de otra entidad (empresas, establecimientos, viviendas, etc.).

- `"variables_identificadoras_list_personas"` (Opcional):
  Lista de columnas que identifican de manera única cada registro individual del ensamble secundario (por ejemplo `["ID_REGISTRO"]`, `["id"]`). Habilita la exportación adicional del ensamble secundario.

- `"variables_a_agrupar_personas"` (Opcional):
  Define la agrupación para los datos del ensamble secundario, siguiendo la misma estructura que `"variables_a_agrupar_lugares"`.


### Salida

El archivo de salida contendrá, para cada grupo, los conteos de cada valor posible de las variables categóricas, el resultado de aplicar la operación especificada a las variables numéricas, y el total de registros que se incluye automáticamente una columna `conteo::total_datos`.
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

En este paso se realiza una clasificación en cuantiles y procesamiento de los datos previamente preprocesasdos y agrupados, se espera que los archivos cumplan con ciertas características mínimas para el correcto funcionamiento del programa, a continuación se describen los dos archivos mínimos, así como las columnas que se pueden reconocer en estos:
1. Archivo de datos
  - Obligatorio
    - Columna que tenga como valores identificadores para cada grupo existente (por ejemplo, municipios, estados, etc.).
2. Archivo de alias
  - Obligatorio
    - Columna que tenga como valores los nombres de las variables tal cual se escriben en las columnas del archivo de datos. Nota: El programa solo procesará variables existente en esta columna, si se especifica una variable que no existe, se omitirá.
    - Columna que tenga como valores los nombres alternativos (alias) de las variables.

### Archivo de configuración

Ejemplo: `config/procesador_example_ensanut.json`

```json
{
    "rutas_csv_mallas": {
        "mun": "../data/covid19/preprocessed/preprocesamiento_covid19_example_mun.csv"
    },
    "rutas_csv_personas": {
        "personas": "../data/covid19/preprocessed/preprocesamiento_covid19_example_mun_personas.csv"
    },
    "ruta_csv_dataframe_alias": "../data/covid19/preprocessed/preprocesamiento_covid19_alias_example_mun.csv",
    "ruta_csv_dataframe_alias_personas": "../data/covid19/preprocessed/preprocesamiento_covid19_alias_example_mun_personas.csv",
    "ruta_csv_salida": "../data/covid19/processed/procesamiento_covid19_example.csv",
    
    "columna_dataframe_alias_nombres": "alias",
    "columna_dataframe_alias_alias": "variable",
    "columna_dataframe_alias_descripcion": "descripcion",

    "variables_identificadoras_lugares": {
        "mun": ["ENTIDAD_RES", "MUNICIPIO_RES"]
    },
    "variables_excluidas_list_lugares": ["ASMA-1"],
    "variables_excluidas_regex_lugares": ["^ENTIDAD.*"],
    "variables_a_procesar_list_lugares": {
        "None": ["media::EDAD"]
    },
    "variables_a_procesar_regex_lugares": {
        "conteo::total_datos": ["^TIPO_PACIENTE.*"]
    },

    "tipo_ensamble": "personas",
    "variables_identificadoras_personas": {
        "personas": ["ID_REGISTRO"]
    },
    "variables_excluidas_list_personas": ["ASMA-1"],
    "variables_excluidas_regex_personas": ["^ENTIDAD.*"],
    "variables_a_procesar_list_personas": {
        "None": ["EDAD"]
    },
    "variables_a_procesar_regex_personas": {
        "None": ["^CLASIFICACION_FINAL_COVID.*"]
    },

    "q": 10
}
```

- `"rutas_csv_mallas"`:  
  Diccionario que indica las rutas a los archivos CSV de entrada para cada malla diferente (por ejemplo, `"mun"` para municipio). Cada clave es el nombre de la malla y el valor es la ruta al archivo correspondiente para lugares.

- `"tipo_ensamble"` (Opcional):
  Nombre del tipo de ensamble secundario. Define el sufijo del archivo de salida generado (por ejemplo, `"personas"` produce `_personas.csv`, `"empresas"` produce `_empresas.csv`). Por defecto es `"personas"`. La clave usada en `"rutas_csv_personas"` y `"variables_identificadoras_personas"` debe coincidir con este valor.

- `"rutas_csv_personas"` (Opcional):
  Diccionario que indica las rutas a los archivos CSV del ensamble secundario. La clave debe coincidir con el valor de `"tipo_ensamble"` (por ejemplo `{"personas": "..."}` o `{"empresas": "..."}`).

- `"ruta_csv_dataframe_alias"`:
  Ruta al archivo CSV que contiene el dataframe de alias de variables y valores original, generado en el preprocesamiento de lugares.

- `"ruta_csv_dataframe_alias_personas"` (Opcional):
  Ruta al archivo CSV que contiene el dataframe de alias originado por el preprocesamiento del ensamble secundario. De no especificarse, se usará el alias general.

- `"ruta_csv_salida"`:
  Ruta base general de guardado para el archivo CSV con resultados. Un archivo con sufijo `_<tipo_ensamble>.csv` se auto-generará si los campos del ensamble secundario están definidos.

- `"columna_dataframe_alias_nombres"`:  
  Nombre de la columna en el dataframe de alias que contiene los nombres descriptivos.

- `"columna_dataframe_alias_alias"`:  
  Nombre de la columna en el dataframe de alias que contiene los alias o nombres cortos.

- `"columna_dataframe_alias_descripcion"` (Opcional):  
  Nombre de la columna en el dataframe de alias que alberga información descriptiva ampliada. Al especificarse, el resultado mantendrá estas descripciones.

- `"variables_identificadoras_lugares"` / `"variables_identificadoras_personas"`:
  Diccionario que mapea cada malla a una lista de columnas identificadoras únicas. La clave en `"variables_identificadoras_personas"` debe coincidir con `"tipo_ensamble"`. Ej: `{"mun": ["ENTIDAD_RES", "MUNICIPIO_RES"]}` / `{"personas": ["ID_REGISTRO"]}`.

- `"variables_excluidas_list_lugares"` / `"variables_excluidas_list_personas"`:  
  Lista explícita de variables (o columnas) a excluir del procesamiento.

- `"variables_excluidas_regex_lugares"` / `"variables_excluidas_regex_personas"`:  
  Lista de expresiones regulares para excluir variables cuyo nombre coincida con algún patrón especificado.

- `"variables_a_procesar_list_lugares"` / `"variables_a_procesar_list_personas"`:  
  Diccionario que indica pares de: variable utilizada como base de normalización (clave), y una lista explícita de variables a procesar (valor). Si la clave es `"None"`, las variables se procesan sin normalizar. Ejemplo:
    ```json
    {
      "None": ["conteo::total_datos"],
      "conteo::total_datos": ["salud_tiene_obesidad-1"]
    }
    ```

- `"variables_a_procesar_regex_lugares"` / `"variables_a_procesar_regex_personas"`:  
  Diccionario que indica pares de: variable utilizada como base de normalización (clave), y una lista de expresiones regulares para englobar variables a procesar (valor). Ejemplo:
    ```json
    {
      "conteo::total_datos": ["^accidente.*"]
    }
    ```

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
python conexion_base/conexion_base_datos.py \
  --ruta-datos-lugares ../data/covid19/processed/procesamiento_covid19_example.csv \
  --ruta-datos-personas ../data/covid19/processed/procesamiento_covid19_example_personas.csv \
  --tipo-ensamble personas \
  --ruta-env ./.env \
  --crear-tabla
```

Para un dataset con ensamble de empresas (DENUE):

```sh
cd src
python conexion_base/conexion_base_datos.py \
  --ruta-datos-lugares ../data/denue/processed/procesamiento_denue_example.csv \
  --ruta-datos-personas ../data/denue/processed/procesamiento_denue_example_empresas.csv \
  --tipo-ensamble empresas \
  --ruta-env ./.env \
  --crear-tabla
```

- `--ruta-datos-lugares`: Ruta del archivo de datos procesados de lugares (mallas) generado en el paso de procesamiento.
- `--ruta-datos-personas`: Ruta del archivo del ensamble secundario generado en el paso de procesamiento. Al especificarse, los datos se cargan en tablas separadas con sufijo `_<tipo-ensamble>`.
- `--tipo-ensamble`: Nombre del tipo de ensamble secundario (por defecto `personas`). Define la clave interna y el sufijo de las tablas en la base de datos. Debe coincidir con el `tipo_ensamble` configurado en el procesador.
- `--ruta-datos-procesados`: (Deprecado) Ruta al archivo CSV con los datos procesados. Se asume que corresponde a datos de lugares si se usa.
- `--ruta-env`: Ruta del archivo de configuración, por defecto `./.env`.
- `--crear-tabla`: No toma ningún valor; si se incluye, se crean las tablas especificadas en caso de no existir en la base de datos.

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

### Salida

Este script automatiza la carga de datos procesados desde los archivos CSV del paso de procesamiento hacia la base de datos PostgreSQL, implementando una normalización automática de tablas. Detecta y transforma tipos de datos avanzados de PostgreSQL, convirtiendo columnas con prefijo `cells_` en arreglos de enteros (`INTEGER[]`) y columnas `interval_` en rangos numéricos (`NUMRANGE`), adaptando formatos como "min:max" a la sintaxis nativa "[min,max)". Adicionalmente, optimiza el esquema separando los metadatos repetitivos en una tabla diccionario (`dict_<tabla>`) y vinculándolos a la tabla principal mediante una clave foránea (`dict_id`).

Las tablas generadas para cada dataset siguen la convención `<DB_TABLE>_<tipo>`, donde `<tipo>` es `mun` para el ensamble de lugares y el valor de `--tipo-ensamble` para el ensamble secundario (por ejemplo `_personas`, `_empresas`). Lo mismo aplica para las tablas auxiliares `dict_<tabla>` y `values_<tabla>`.

---

## Scripts de transformación

En la carpeta [`scripts/`](scripts/) se incluyen scripts de transformación de datasets crudos, pensados para ejecutarse como paso previo al preprocesamiento cuando la estructura original del dataset no es directamente compatible con el flujo principal.

Cada script corresponde a una fuente de datos específica y genera un archivo transformado listo para ser procesado por el preprocesador.

| Script | Dataset | Descripción |
|---|---|---|
| `transform_denue_muestra.py` | DENUE | Transforma el dataset transaccional del DENUE generando un `id` reversible a partir de coordenadas (base64), conservando `cve_mun_resumido` y construyendo `var` como `{codigo_act}_{year}` |

---

## Notebooks

En la carpeta [`notebooks/`](notebooks/) se incluyen los procedimientos de generación de archivos de diccionarios de datos compatibles con el paso de preprocesamiento para las fuentes de datos de ejemplo.

---
