import os

def save_data(data, filepath):
  ensure_dir_exists(os.path.dirname(filepath))
  with open(filepath, 'w') as file:
    file.write(data)

def ensure_dir_exists(dir):
  if not os.path.isdir(dir):
      os.makedirs(dir)


