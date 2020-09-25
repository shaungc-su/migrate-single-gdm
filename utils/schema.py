import json
from pathlib import Path
import os


class SchemaManager:
    SCHEMA_DIRECTORY = Path(f"{os.environ.get('GCI_VCI_SERVERLESS_RELATIVE_PATH', '')}/models")
    def __init__(self):
        if not os.environ.get('GCI_VCI_SERVERLESS_RELATIVE_PATH'):
            raise Exception(f'ERROR: Schema directory not found: env var `GCI_VCI_SERVERLESS_RELATIVE_PATH` not set')

        if not self.SCHEMA_DIRECTORY.is_dir():
            raise Exception(f'ERROR: Schema directory not found: `{self.SCHEMA_DIRECTORY.absolute()} is not a directory`')
    
    def read_schema_from_file(self, item_type):
        with open(self.SCHEMA_DIRECTORY.joinpath(f'{item_type}.json')) as f:
            schema = json.load(f)['properties']
        
        # patch schema of gdm.annotations so that we know how to link gdm to annotation
        # but don't change sls json schema for gdm, since controller
        # use it to populate field, and we don't want to populate gdm.annotations
        if item_type == 'gdm':
            schema['annotations']['items']['$schema'] = 'annotation'
        
        return schema
