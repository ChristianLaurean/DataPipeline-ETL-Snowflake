# DataPipeline-ETL-Snowflake

![Diagrama del Pipeline ETL](https://github.com/ChristianLaurean/my_obsidian/blob/main/01%20-%20Projects/ETL%20Basico/Dise%C3%B1o_ETL.excalidraw.png)

Este repositorio contiene un Data Pipeline ETL (Extract, Transform, Load) que extrae datos de varias fuentes, los transforma y los carga en una base de datos Snowflake como parte de un proceso de ETL.

## Estructura del Proyecto

El proyecto está estructurado de la siguiente manera:

- **dockerfile**: Contiene el archivo Dockerfile para la configuración del contenedor.
- **etl**: Directorio principal del código fuente del pipeline ETL.
  - **src**: Contiene los archivos de origen de datos.
  - **constants.py**: Archivo que define las constantes utilizadas en el pipeline.
  - **pipeline_utils.py**: Módulo que contiene las funciones de utilidad para el pipeline.
  - **etl_pipeline.py**: Script principal que orquesta el proceso de ETL.

## Requisitos

Antes de ejecutar el pipeline, asegúrate de tener instalado Docker y haber configurado las variables de entorno necesarias para la conexión a la base de datos OLTP y Snowflake.

## Uso

1. Clona este repositorio en tu máquina local.
2. Configura las variables de entorno necesarias en un archivo `.env` en el directorio raíz del proyecto.
3. Ejecuta el siguiente comando para construir la imagen Docker:

```bash
docker build -t nombre_imagen .
```

Una vez que la imagen Docker se haya construido correctamente, ejecuta el siguiente comando para crear y ejecutar un contenedor basado en la imagen:

```bash
docker run nombre_imagen
```
