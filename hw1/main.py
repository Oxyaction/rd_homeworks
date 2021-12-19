import os 
from config import Config
from api import Api
from file_data_manager import save_data

def main(request_date):
  config = Config('./config.yml')
  api = Api(config)

  api.set_access_token()
  data = api.get_data(request_date)

  filepath = get_file_name(config.get_save_dir(), f'{request_date}.json')
  save_data(data, filepath)


def get_file_name(savedir, filename):
  scriptdir = os.path.dirname(os.path.realpath(__file__))
  return os.path.join(scriptdir, savedir, filename)

if __name__ == '__main__':
  request_date = '2021-12-01'
  main(request_date)
  