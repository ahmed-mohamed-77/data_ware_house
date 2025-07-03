from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests

@dag
def user_processing():
    create_table=SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


    # sensor function is a testing function is waiting to the function is true to succeed
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        
        # validate the api for working or not
        api=r"https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
    
        response = requests.get(api)
        if response.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=response.json())
        return PokeReturnValue(is_done=False, xcom_value=None)
    is_api_available()
    

user_processing()