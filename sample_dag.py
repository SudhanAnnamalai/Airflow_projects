from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Sudharsan Annamalai',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def greet():
  print("Hello World!")


with DAG(
  default_args = default_args,
  dag_id = 'Python Operator Experiment',
  description = 'Dag to test the python Operator, and its functionalities',
  start_date = datetime(2024, 1, 03),
  schedule_interval = "@hourly") as dag:
    task_1 = PythonOperator(
      task_id = "greet_python",
      pythoncallable = greet
    )
task_1
    

  
