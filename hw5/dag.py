from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from table_dump_operator import TableDumpOperator
from hdfs_upload import hdfs_upload


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'dump_dshop_hdfs',
    description='Sample DAG',
    schedule_interval='@daily',
    start_date=datetime(2022,2,8,0,0),
    default_args=default_args
)

tables = [
    'aisles',
    'clients',
    'departments',
    'location_areas',
    'orders',
    'products',
    'store_types',
    'stores',
]

dump_tables = []

for table in tables:
    dump_tables.append(TableDumpOperator(
        task_id = f'dump-table-{table}',
        table_name=table,
        save_path='/home/airflow/data',
        postgres_conn_id='postgres_dshop_full',
        dag=dag
    ))


upload_hdfs = PythonOperator(
    task_id='upload-hdfs',
    op_kwargs={
        'files': [f'{table}.csv' for table in tables],
        'from_base_path': '/home/airflow/data',
        'to_base_path': '/bronze',
        'hadoop_conn_id': 'hadoop',
    },
    provide_context=True,
    python_callable=hdfs_upload,
    dag=dag
)

dump_tables >> upload_hdfs
