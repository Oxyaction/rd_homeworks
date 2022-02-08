from airflow.hooks.base_hook import BaseHook
from airflow.operators.postgres_operator import PostgresHook
from airflow.models import BaseOperator

class TableDumpOperator(BaseOperator):
  def __init__(self, table_name, save_path, postgres_conn_id='postgres_default', database=None, *args, **kwargs):
      super().__init__(*args,**kwargs)
      self.table_name = table_name
      self.save_path = save_path
      self.postgres_conn_id = postgres_conn_id
      self.database = database

  def execute(self, context):
    file = f'{self.save_path}/{self.table_name}.csv'
    sql = f'COPY "{self.table_name}" TO STDOUT WITH HEADER CSV'
    
    self.log.info('Copying table "%s" to "%s"', self.table_name, file)
    
    self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                              schema=self.database)
    self.hook.copy_expert(sql, file)
      