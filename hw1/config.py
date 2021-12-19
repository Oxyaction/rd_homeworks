import yaml, os

class Config:
  def __init__(self, filename) -> None:
    with open(filename, 'r') as yaml_file:
      self.__config = yaml.load(yaml_file, yaml.FullLoader)
  
  def get_url(self) -> str:
    return self.__config['url']

  def get_username(self) -> str:
    return self.__config['authentication']['username']
  
  def get_password(self) -> str:
    return os.environ['API_PASSWORD']

