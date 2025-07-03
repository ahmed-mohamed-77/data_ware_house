from typing import LiteralString
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
# from airflow.utils.dates import days_age
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv("data_ware_house\.env", override=True)
postgres_string_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

"""the DAG (Directed Acyclic Graph)
------------------------------------------------------
A Dag is a collection of all the tasks you want to run, organized in a way that reflect their
dependencies.
it helps you defines the structure of your entire workflow, showing which tasks needs to happen before others

CONCEPT 2: Operator
an operator defines a single, ideally idempotent, task in your dag

CONCEPT 3: Task / Task Instance
A task is a specific instance of an operator. when an operator is assigned to a dag, it becomes a task
Tasks are the actual units of work that get executed when your dag runs

CONCEPT 4: Workflow
A workflow is the entire process defined by your DAG, including all tasks and their dependencies

"""

# latidute and longtude
LAT="51.5074"
LONG="-0.1278"


default_arg={
    "owner": "airflow",
    "start_date" : datetime(2025, 1, 1)
}

API_CONN_ID="open_meteo_api"

with DAG(
    dag_id="weather_etl",
    default_args=default_arg,
    catchup=False
) as dags:
    
    @task()
    def extract_weather_data():
        """_summary_
            Extract weather data from open metro api using airflow connection
        """
        
        # use http hook to get connection details from airflow connection
        http_hook=HttpHook(http_conn_id=API_CONN_ID, method="GET")
        
        # build api endpoint
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint: LiteralString=f'/v1/forecast?latitude={LAT}&longitude={LONG}&current_weather=true'
        
        response=http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LAT,
            'longitude': LONG,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into postgres"""
        pg_hook = PostgresHook("postgres_default")
        conn=pg_hook.get_conn()
        cursor = conn.cursor()
        
        
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()
        
        ## DAG Worflow- ETL Pipeline
        weather_data= extract_weather_data()
        transformed_data=transform_weather_data(weather_data)
        load_weather_data(transformed_data)
