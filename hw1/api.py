import requests
from config import Config

class Api:
  __token: str

  def __init__(self, config: Config):
      self.__config = config

  def set_access_token(self):
    self.__config.get_url()

    result = requests.post(
      f'{self.__config.get_url()}/auth',
      json={'username': self.__config.get_username(), 'password': self.__config.get_password()}, 
      headers={'Content-Type': 'application/json'}
    )

    if result.status_code != 200:
      raise 'Incorrect get token response ' + result.status_code

    self.__token = result.json()['access_token']
    

  def get_data(self, request_date):
    result = requests.get(
      f'{self.__config.get_url()}/out_of_stock',
      json={'date': request_date}, 
      headers={'Content-Type': 'application/json', 'Authorization': f'JWT {self.__token}'}
    )

    return result.text
