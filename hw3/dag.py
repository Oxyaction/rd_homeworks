from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from products import get_products


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'sample_dag',
    description='Sample DAG',
    schedule_interval='@daily',
    start_date=datetime(2022,1,28,0,0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='download-products',
    op_kwargs={'api_password': Variable.get('api_password')},
    python_callable=get_products,
    provide_context=True,
    dag=dag
)

t1
