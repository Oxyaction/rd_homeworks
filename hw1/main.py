from config import Config
from api import Api

def main():
  config = Config('./config.yml')
  api = Api(config)

  api.set_access_token()
  data = api.get_data()
  print(data)


if __name__ == '__main__':
  main()