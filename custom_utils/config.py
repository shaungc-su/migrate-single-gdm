import yaml
import os

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))

with open(f'{THIS_FILE_DIR}/../config_recent.yaml', 'r') as stream:
    config_data = yaml.load(stream, Loader=yaml.FullLoader)