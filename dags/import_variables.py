import json
import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Settings
DAG_ID = 'import_variables_from_json'
VARIABLES_FILE = 'variables.json'  # File is expected to be in the DAGs folder

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_variables(**kwargs):
    # The DAGs folder is usually the base path for relative file access in Airflow
    # or we can find it relative to this file
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(dag_folder, VARIABLES_FILE)
    
    logging.info(f"Looking for variables file at: {file_path}")
    
    if not os.path.exists(file_path):
        logging.warning(f"File {file_path} not found. Skipping import.")
        return

    try:
        with open(file_path, 'r') as f:
            vars_dict = json.load(f)
            
        logging.info(f"Found {len(vars_dict)} variables to import.")
        
        for key, value in vars_dict.items():
            if value is not None:
                # Serialize complex types (dicts/lists) to JSON string if needed, 
                # but Airflow Variables are strings. 
                # If the value is a dict/list, we might want to store it as JSON.
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                else:
                    value = str(value)
                
                Variable.set(key, value)
                logging.info(f"Set variable: {key}")
            else:
                logging.info(f"Skipping null value for key: {key}")
                
        logging.info("Variables import completed successfully.")
        
    except Exception as e:
        logging.error(f"Failed to import variables: {e}")
        raise

with DAG(
    DAG_ID,
    default_args=default_args,
    description='Imports variables from variables.json uploaded to DAGs folder',
    schedule_interval='@once', # Run once when enabled
    catchup=False,
    tags=['setup', 'mlops'],
) as dag:

    import_task = PythonOperator(
        task_id='load_variables_from_json',
        python_callable=load_variables,
    )

    import_task
