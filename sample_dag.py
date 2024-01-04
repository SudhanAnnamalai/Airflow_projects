from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Sudharsan Annamalai',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def greet(name, age):                                                                    #name, age as dependency parameters.
  print(f"Hi my name is {name} \n I am {age} years Old!, \n I am about to Have a baby")


with DAG(
  default_args = default_args,
  dag_id = 'Baby_Callable',
  description = 'Dag to test the python Operator, and its functionalities',
  start_date = datetime(2024, 1, 3),
  schedule_interval = "@hourly") as dag:
    task_1 = PythonOperator(
      task_id = "greet_python",
      python_callable = greet,
      op_kwargs = {'name': 'Vijishree', 'age': 28}
    )
task_1
    

  
