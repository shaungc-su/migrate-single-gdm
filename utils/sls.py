from requests_aws4auth import AWS4Auth
import os
import requests
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import pathlib
import shutil

from utils.relation_linkage import get_pk_or_rid


# item_type -> endpoint
POST_ENDPOINTS = {
    'gdm': '/gdms',
    'annotation': '/annotations',

    'user': '/users',
    'article': '/articles',
    'disease': '/diseases',
    'gene': '/genes',
    'evidenceScore': '/evidencescore',

    'individual': '/individuals',
    'family': '/families',
    'group': '/groups',
    'experimental': '/experimental',
    'caseControl': '/casecontrol',
}

class Serverless:
    def __init__(self, logger, base_url='http://0.0.0.0:3000'):
        self.auth=AWS4Auth(os.getenv('AWS_ACCESS_KEY_ID'), \
            os.getenv('AWS_SECRET_ACCESS_KEY'),'us-west-2', \
                'execute-api')

        self.logger = logger
        self.BASE_URL = base_url

        self.logger.info(f'Will connect to serverless endpoint {base_url}')
    
    def remove_empty_fields(self, parent):
        processed_parent = {**parent}
        for field_name, field_value in parent.items():
            if isinstance(field_value, str) and field_value == '':
                del processed_parent[field_name]
            elif isinstance(field_value, dict):
                processed_parent[field_name] = self.remove_empty_fields(field_value)
            elif isinstance(field_value, list):
                processed_parent[field_name] = [self.remove_empty_fields(list_item) if isinstance(list_item, dict) else list_item for list_item in field_value if field_value != '']
        
        return processed_parent
    
    def get(self, parent):
        item_type = parent['item_type']
        res = requests.get(f'{self.BASE_URL}{POST_ENDPOINTS[item_type]}/{get_pk_or_rid(parent)}', auth=self.auth)
        if res.status_code == 404:
            return None
        
        if not res.ok:
            self.logger.error(res.text)
            res.raise_for_status()
        
        if not res.text:
            raise Exception(f'GetError: {res.status_code} {item_type} {get_pk_or_rid(parent)} empty response, parent = {parent}')
            
        data = res.json()

        if not data:
            raise Exception(f'GetError: {res.status_code} {item_type} {get_pk_or_rid(parent)} empty json body, parent = {parent}')
        
        if not isinstance(data, dict):
            raise Exception(f'GetError: {res.status_code} {item_type} {get_pk_or_rid(parent)} json body is not object (dict), response = {data}')

        # some endpoints like disease, gene, etc, will return data regardless of object exist in db or not
        # but we can tell if it's in db by checking if PK is assigned
        if not 'PK' in data:
            self.logger.debug(f'GET {item_type} responded with data but no PK, so no object in db yet')
            return None
        
        return data
        
    def post(self, parent):
        processed_parent = self.remove_empty_fields(parent)

        item_type = processed_parent['item_type']
        data = {
            'body': processed_parent
        }
        res = requests.post(
            f'{self.BASE_URL}{POST_ENDPOINTS[item_type]}',
            auth=self.auth,
            data=json.dumps(data)
        )
        if not res.ok:
            # assume 422 error is duplicate PK object creation in db
            if res.status_code == 422 and 'The conditional request failed' in res.text:
                self.logger.info(f'skipping 422 for {item_type} {processed_parent["rid"]}, probably object already exist in db')
                return

            raise Exception(f'PostError: {res.status_code} {item_type} fail to post, sls response = {res.text}, parent = {processed_parent}')

        if not res.text:
            raise Exception(f'PostError: {res.status_code} {item_type} got empty response after posted, parent = {processed_parent}')
            
        return res

class DynamoDB:
    TABLE_NAME = 'GeneVariantCuration-dev'
    def __init__(self, logger):
        self.db = boto3.resource('dynamodb',
            aws_access_key_id="anything",
            aws_secret_access_key="anything",
            region_name="us-west-2",
            endpoint_url="http://localhost:8000"
        ).Table(self.TABLE_NAME)

        self.logger = logger
    
    def get(self, item):
        '''
            AWS doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html#GettingStarted.Python.03.02
        '''
        
        item_type = item['item_type']
        pk = get_pk_or_rid(item)
        
        res = self.db.query(
            # Note: only AND can be used, OR is not available for `KeyConditionExpression`
            KeyConditionExpression=Key('PK').eq(pk),
            FilterExpression=Attr('item_type').eq(item_type)
        )

        if res['ResponseMetadata']['HTTPStatusCode'] == 200:
            items = res['Items']
            if len(items) == 0:
                return None
            if len(items) != 1:
                raise Exception(f'GetError: {item_type} result is not one: {items}')
            return items[0]
        else:
            return None
    
    def reset(self):
        path = pathlib.Path('../gci-vci-aws/gci-vci-serverless/.dynamodb/data/shared-local-instance.db')
        user_migrated_data_path = pathlib.Path(path.parent.parent.joinpath('data_backup/shared-local-instance__user_migrated.db'))
        if path.exists():
            os.remove(path.absolute())
            self.logger.info('removed existing db data')
            if user_migrated_data_path.exists():
                shutil.copy(user_migrated_data_path, path)
                self.logger.info('Database reset complete')
            else:
                self.logger.error(f'Database reset failed: {user_migrated_data_path.absolute()} does not exist')
        else:
            self.logger.error(f'Database reset failed: {path.absolute()} does not exist')