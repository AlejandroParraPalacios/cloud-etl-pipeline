# Meetup ETL Pipeline
---
Este proyecto implementa un pipeline de procesamiento de datos utilizando `Apache Airflow`, `Snowflake`, y `AWS S3`. Automatiza la transformación y exportación de datos desde archivos CSV hacia una arquitectura moderna de datos en la nube.

## 📂 Estructura del Proyecto

PROJECT_5/
├── Data/ # Archivos CSV originales del dataset de Meetup
├── Pipeline/ # Configuración y orquestación de Airflow
│ ├── dags/ # DAGs de Airflow
│ ├── config/ # Configuración de Airflow
│ ├── plugins/ # Funciones externas y configuraciones
│ ├── logs/ # Logs generados por Airflow
│ ├── .env.example # Variables de entorno de ejemplo
│ ├── docker-compose.yaml # Entorno Docker para Airflow
│ └── *.json, *.sql # Scripts SQL y políticas AWS
├── Querys/ # Scripts SQL por tabla (para Snowflake)
└── README.md

## ⚙️ Tecnologías utilizadas

- Apache Airflow (en Docker)
- Snowflake
- Amazon S3
- Python 3.12
- Slack (para alertas)

## 🔁 DAG de Airflow

El DAG ejecuta las siguientes tareas cada 15 minutos:

1. **Crear tablas** en Snowflake (si no existen)
2. **Transformar datos** desde tablas RAW a STG
3. **Cargar datos nuevos** usando `MERGE`
4. **Notificar en Slack** al finalizar (éxito o error)
5. **Exportar tabla final a S3** en formato CSV

## 📤 Exportación a S3

Snowflake exporta la tabla `MEETUP_FINAL` al bucket `s3://meetup-pipeline-output/` utilizando una `STORAGE INTEGRATION`.

## 🔐 Seguridad

Los siguientes archivos están ignorados por `.gitignore` para proteger información sensible:

- `.env`
- Archivos `.json` con políticas AWS
- Claves y scripts privados

## 🚀 Cómo iniciar

1. Clona el repositorio
2. Copia el archivo `.env.example` a `.env` y completa tus credenciales
3. Ejecuta el entorno Airflow:

```bash
cd Pipeline
docker compose up --build