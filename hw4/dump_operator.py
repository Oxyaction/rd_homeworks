from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator

class DumpOperator(BashOperator):
  def __init__(self, postgres_conn_id='postgres_default', dump_path='/home/airflow/data/dump.sql', *args, **kwargs):
      super().__init__(*args,**kwargs)
      self.postgres_conn_id = postgres_conn_id
      self.dump_path = dump_path

  def execute(self, context):
    connection = BaseHook.get_connection(self.postgres_conn_id)
    bash_command = f'pg_dump {connection.get_uri()} > {self.dump_path}'
    self.bash_command = bash_command
    return super().execute(context)