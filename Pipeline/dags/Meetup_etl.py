from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
import requests
import snowflake.connector
import os

# Functions and Environment Variables
SNOWFLAKE_CONGIF = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': 'MEETUP_SCHEMA'
}

SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

# Create tables in Snowflake
def create_tables():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONGIF)
    cs = conn.cursor()
    try:
        cs.execute("""
            CREATE TABLE IF NOT EXISTS STG_EVENTS (
                event_id STRING,
                event_name STRING,
                created_at TIMESTAMP,
                event_time TIMESTAMP,
                group_id STRING,
                venue_id STRING,
                city STRING,
                country STRING,
                latitude FLOAT,
                longitude FLOAT
            );
            CREATE TABLE IF NOT EXISTS STG_GROUPS (
                group_id STRING, 
                group_name STRING, 
                category_id STRING,
                city STRING, 
                country STRING
            );
            CREATE TABLE IF NOT EXISTS STG_CITIES (
                city STRING, 
                country STRING, 
                state STRING,
                latitude FLOAT, 
                longitude FLOAT
            );
            CREATE TABLE IF NOT EXISTS STG_VENUES (
                venue_id STRING, 
                venue_name STRING, 
                address_1 STRING,
                city STRING, 
                country STRING, 
                lat FLOAT, 
                lon FLOAT
            );
            CREATE TABLE IF NOT EXISTS MEETUP_FINAL LIKE STG_EVENTS;
        """)
    finally:
        cs.close()
        conn.close()
    print("Tables created successfully.")

# Data Transformation
def transform_data():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONGIF)
    cs = conn.cursor()
    try:
        cs.execute("""
            CREATE OR REPLACE TEMP TABLE NEW_STG_EVENTS AS
                SELECT
                   event_id, 
                   INITCAP(event_name), 
                   TO_TIMESTAMP(created) AS created_at,
                   TO_TIMESTAMP(event_time) AS event_time,
                   group_id,
                   venue_id,
                   venue_city AS city, 
                   venue_country AS country,
                   venue_lat AS latitude, 
                   venue_lon AS longitude
                FROM RAW_EVENTS;

            CREATE OR REPLACE TEMP TABLE NEW_STG_GROUPS AS
                SELECT 
                   group_id, 
                   INITCAP(group_name),
                   category_id, 
                   city, 
                   country 
                FROM RAW_GROUPS;

            CREATE OR REPLACE TEMP TABLE NEW_STG_CITIES AS
                SELECT
                   city,
                   country,
                   state,
                   latitude,
                   longitude 
                FROM RAW_CITIES;

            CREATE OR REPLACE TEMP TABLE NEW_STG_VENUES AS
                SELECT 
                   venue_id, 
                   INITCAP(venue_name), 
                   address_1, 
                   city, 
                   country, 
                   lat, 
                   lon 
                FROM RAW_VENUES;
        """)
    finally:
        cs.close()
        conn.close()
    print("Data transformed successfully.")


# Data Loading
def load_data():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONGIF)
    cs = conn.cursor()
    try:
        cs.execute("""
            MERGE INTO STG_EVENTS t USING NEW_STG_EVENTS s
            ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT VALUES (s.*);

            MERGE INTO STG_GROUPS t USING NEW_STG_GROUPS s
            ON t.group_id = s.group_id
            WHEN NOT MATCHED THEN INSERT VALUES (s.*);

            MERGE INTO STG_CITIES t USING NEW_STG_CITIES s
            ON t.city = s.city AND t.country = s.country
            WHEN NOT MATCHED THEN INSERT VALUES (s.*);

            MERGE INTO STG_VENUES t USING NEW_STG_VENUES s
            ON t.venue_id = s.venue_id
            WHEN NOT MATCHED THEN INSERT VALUES (s.*);

            CREATE OR REPLACE TABLE MEETUP_FINAL AS
            SELECT
                e.event_id, e.event_name, e.created_at, e.event_time,
                e.city, e.country, g.group_name, v.venue_name,
                c.state AS city_state, e.latitude, e.longitude
            FROM STG_EVENTS e
            LEFT JOIN STG_GROUPS g ON e.group_id = g.group_id
            LEFT JOIN STG_VENUES v ON e.venue_id = v.venue_id
            LEFT JOIN STG_CITIES c ON e.city = c.city AND e.country = c.country;
        """)
    finally:
        cs.close()
        conn.close()
    print("Data loaded successfully.")

# Send Slack Notification
def slack_success_alert(context):
    if SLACK_WEBHOOK_URL:
        message = {"text": f":white_check_mark: DAG *{context['dag'].dag_id}* finalizado correctamente."}
        requests.post(SLACK_WEBHOOK_URL, json = message)
    print("Slack notification sent successfully.")

def slack_failure_alert(context):
    message = {
            "text": f":x: DAG *{context['dag'].dag_id}* falló en la tarea *{context['task_instance'].task_id}*\nRazón: {context.get('exception')}"
        }
    requests.post(SLACK_WEBHOOK_URL, json = message)
    print("Slack notification sent unsuccessfully.")

# Export a table to S3
def export_to_s3():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONGIF)
    cs = conn.cursor()
    try:
        cs.execute("""
            REMOVE @stage_meetup_s3;
            COPY INTO @stage_meetup_s3/meetup_final.csv
            FROM MEETUP_FINAL
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' HEADER=TRUE OVERWRITE=TRUE);
        """)
    finally:
        cs.close()
        conn.close()
    print("Data exported to S3 successfully.")

# DAG Definition
with DAG(
    dag_id = 'meetup_etl_pipeline',
    default_args = {
        'owner': 'airflow',
        'retries': 1,
        'on_failure_callback': slack_failure_alert,
        'on_success_callback': slack_success_alert
    },
    start_date = datetime.now() - timedelta(days = 1),
    schedule = '*/15 * * * *',
    catchup = False,
    tags = ['meetup', 'etl', 'snowflake']
) as dag:
    
    t1 = PythonOperator(task_id = 'create_tables', python_callable = create_tables)
    t2 = PythonOperator(task_id = 'transform_data', python_callable = transform_data)
    t3 = PythonOperator(task_id = 'load_data', python_callable = load_data)
    t4 = PythonOperator(task_id = 'export_to_s3', python_callable = export_to_s3)

    t1 >> t2 >> t3 >> t4