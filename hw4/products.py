import os 
from api import Api
from file_data_manager import save_data

def get_products(**kwargs):
  request_date = '2021-12-01'
  api = Api(kwargs['api_password'])

  api.set_access_token()
  data = api.get_data(request_date)

  filepath = get_file_name('/home/airflow/data', f'{request_date}.json')
  save_data(data, filepath)


def get_file_name(savedir, filename):
  scriptdir = os.path.dirname(os.path.realpath(__file__))
  return os.path.join(scriptdir, savedir, filename)
