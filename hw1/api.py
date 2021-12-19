import requests, sys
from requests import RequestException
from config import Config

class Api:
  __token: str

  def __init__(self, config: Config):
      self.__config = config

  def set_access_token(self):
    self.__config.get_url()

    try:
      result = requests.post(
        f'{self.__config.get_url()}/auth',
        json={'username': self.__config.get_username(), 'password': self.__config.get_password()}, 
        headers={'Content-Type': 'application/json'}
      )

      json = result.json()

      if ('error' in json.keys()):
        raise IncorrectRequestException(json['status_code'], json['description'], json['error'])

      self.__token = ['access_token']
    except (RequestException, IncorrectRequestException) as e:
      print('Requesting token error', e, file=sys.stderr)
    
    

  def get_data(self, request_date):
    try:
      result = requests.get(
        f'{self.__config.get_url()}/out_of_stock',
        json={'date': request_date}, 
        headers={'Content-Type': 'application/json', 'Authorization': f'JWT {self.__token}'}
      )
    except (RequestException, IncorrectRequestException) as e:
      print('Requesting data error', e, file=sys.stderr)

    return result.text

class IncorrectRequestException(Exception):
  def __init__(self, status_code, description, error) -> None:
      self.__status_code = status_code
      self.__description = description
      self.__error = error

  def __str__(self) -> str:
      return f'[{self.__status_code}] {self.__error}. {self.__description}'