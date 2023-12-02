from datetime import datetime 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# аргументы дага по умолчанию
default_args = {
    "owner": "momiv",
    "retries": 0,
    "start_date": datetime.today()
}

with DAG(dag_id="01_conn_var", 
         default_args=default_args, 
         description="init var, conn",
         schedule_interval='@once',
         catchup=False) as dag:

    start = EmptyOperator(task_id='start') 
   
    set_var = BashOperator(
        task_id='set_var',
        bash_command= 'airflow variables import /opt/airflow/dags/variables.json'
    )

    set_conn = BashOperator(
        task_id='set_conn',
        bash_command= 'airflow connections import /opt/airflow/dags/connections.json'
    )

    end = EmptyOperator(task_id='end')

    start >> set_var >> set_conn >> end

