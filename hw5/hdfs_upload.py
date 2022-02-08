from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook


def hdfs_upload(files, from_base_path, to_base_path, hadoop_conn_id, **kwargs):
  conn = BaseHook.get_connection(hadoop_conn_id)
  connection = f'http://{conn.host}:{conn.port}/'
  client = InsecureClient(connection, user='user')

  for file in files:
    from_path = f'{from_base_path}/{file}'
    to_path = f'{to_base_path}/{kwargs["ds"]}/{file}'
    client.upload(to_path, from_path, overwrite=True)
