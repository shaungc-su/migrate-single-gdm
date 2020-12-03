from requests_aws4auth import AWS4Auth
import os
import datetime

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import pathlib
import shutil

from custom_utils.config import config_data
from custom_utils.logger import Logger, Estimate
from custom_utils.relation_linkage import get_pk_or_rid

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))

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

    'variant': '/variants',

    'provisionalClassification': '/provisional-classifications',
    'snapshot': '/snapshots',

    'assessment': '/assessments',
    'pathogenicity': '/pathogenicity'
}

CONCURRENT_GET_DEFAULT_THREADS = 10
CONCURRENT_POST_DEFAULT_THREADS = 1
class Serverless:
    POST_ERROR_LOG_FILE = pathlib.Path(f'{THIS_FILE_DIR}/../.log/sls_post_error.log')

    RETRIES_CONFIG = Retry(total=5,
        backoff_factor=0.17,
        # status_forcelist=[ 500, 502, 503, 504 ]
    )

    def __init__(self, logger, base_url='http://0.0.0.0:3000'):
        self.auth=AWS4Auth(os.getenv('AWS_ACCESS_KEY_ID'), \
            os.getenv('AWS_SECRET_ACCESS_KEY'),'us-west-2', \
                'execute-api')

        self.logger = logger
        self.BASE_URL = base_url

        self.logger.info(f'Will connect to serverless endpoint {base_url}')

        # TODO: when running two scripts both use sls.py, this will accidentally delete a process's log file
        # better delete the log manually if want to reset. May find better ways to reset log file in the future
        #
        # if self.POST_ERROR_LOG_FILE.exists():
        #     os.remove(self.POST_ERROR_LOG_FILE.absolute())
    
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
    
    def _handle_get_response(self, res, parent, item_type):
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
    
    def get(self, parent) -> dict:
        item_type = parent['item_type']
        res = requests.get(f'{self.BASE_URL}{POST_ENDPOINTS[item_type]}/{get_pk_or_rid(parent)}', auth=self.auth)
        
        return self._handle_get_response(res, parent, item_type)
    
    def concurrent_get(self, parents: list, threads=CONCURRENT_GET_DEFAULT_THREADS, include_parent_in_result=False, log=False, reduce_log_mod=1) -> list:
        request_list = []
        session = FuturesSession(executor=ThreadPoolExecutor(max_workers=threads))

        for parent in parents:
            item_type = parent['item_type']
            future_request = session.get(f'{self.BASE_URL}{POST_ENDPOINTS[item_type]}/{get_pk_or_rid(parent)}', auth=self.auth)

            future_request.parent = parent
            future_request.item_type = item_type

            request_list.append(future_request)
        
        get_results = []
        estamator = Estimate()
        for index, completed_request in enumerate(as_completed(request_list)):
            res = completed_request.result()

            get_result = self._handle_get_response(
                res, completed_request.parent, completed_request.item_type
            )
            if include_parent_in_result:
                get_result = (get_result, completed_request.parent)
            get_results.append(get_result)

            if log and index % reduce_log_mod == 0:
                progress, eta = estamator.get(index + 1, len(request_list))
                log_message = f'GET {res.status_code} variant({completed_request.parent["rid"]}) processed {index+1}/{len(request_list)}({progress}%) requests ETA {eta}'
                self.logger.info(log_message)
        
        return get_results

    def _prepare_post_kwargs(self, parent):
        item_type = parent['item_type']
        
        data = {
            'body': parent
        }
        querysting_params = {}
        if item_type == 'snapshot':
            resourceParent = parent.get('resourceParent', {})
            if isinstance(resourceParent, dict) and len(resourceParent.keys()) == 1:
                iterator = iter(resourceParent)
                parent_type = next(iterator)
            else:
                raise Exception(f'PostServerPreparationError: cannot tell what parent type is for snapshot; a resourceParent is required by snapshot controller to create: {parent}')
            querysting_params['type'] = parent_type
            querysting_params['action'] = ''

        return {
            'auth': self.auth,
            'data': json.dumps(data),
            'params': querysting_params
        }
    
    def _handle_post_response(self, res, parent, item_type, raise_http_error):
        if not res.ok:
            # assume 422 error is duplicate PK object creation in db
            if res.status_code == 422 and 'The conditional request failed' in res.text:
                self.logger.info(f'skipping 422 for {item_type} {parent["rid"]}, probably object already exist in db')
                return
            
            error_message = f'PostError: {res.status_code} {item_type} fail to post, sls response = {res.text}, parent = {parent}'
            if raise_http_error:
                raise Exception(error_message)
            else:
                now = datetime.datetime.now()
                with self.POST_ERROR_LOG_FILE.open('a') as f:
                    f.write(f'{now.isoformat()}: ' + error_message + '\n\n')
                return res

        if not res.text:
            error_message = f'PostError: {res.status_code} {item_type} got empty response after posted, parent = {parent}'
            raise Exception(error_message)
            
        return res
        
    def post(self, parent, raise_http_error=True):
        processed_parent = self.remove_empty_fields(parent)
        item_type = processed_parent['item_type']

        res = requests.post(
            f'{self.BASE_URL}{POST_ENDPOINTS[item_type]}',
            **self._prepare_post_kwargs(processed_parent)
        )
        
        return self._handle_post_response(res, processed_parent, item_type, raise_http_error)
    
    def concurrent_post(self, parents: list, threads=CONCURRENT_POST_DEFAULT_THREADS, raise_http_error=False, log=False) -> list:
        request_list = []
        session = FuturesSession(executor=ThreadPoolExecutor(max_workers=threads))
        session.mount('http://', HTTPAdapter(max_retries=self.RETRIES_CONFIG))

        for parent in parents:
            processed_parent = self.remove_empty_fields(parent)
            item_type = processed_parent['item_type']

            future_request = session.post(
                f'{self.BASE_URL}{POST_ENDPOINTS[item_type]}',
                **self._prepare_post_kwargs(processed_parent)
            )
            future_request.parent = processed_parent
            request_list.append(future_request)
        
        post_results = []
        estimator = Estimate()
        for index, completed_request in enumerate(as_completed(request_list)):
            res = completed_request.result()

            if log:
                progress, eta = estimator.get(index + 1, len(request_list))
                log_message = f'POST {res.status_code} variant({completed_request.parent["rid"]}) processed {index+1}/{len(request_list)}({progress}%) requests ETA {eta}'
                if res.status_code < 400:
                    self.logger.info(log_message)
                else:
                    self.logger.error(log_message)
        
            post_results.append(self._handle_post_response(
                res, completed_request.parent, completed_request.parent['item_type'], raise_http_error
            ))
        
        return post_results

SLS = Serverless(Logger, base_url=config_data['endpoint']['url'])

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

DYNAMODB = DynamoDB(Logger)