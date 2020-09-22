from multiprocessing import Process
import psycopg2
import time
import os
import psycopg2
import json
import yaml
import sys
from requests_aws4auth import AWS4Auth
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import pathlib
import traceback

from error_tracker import ErrorTracker

error_tracker = ErrorTracker()

class VariantMigrator:
    RECORD_FILENAME = 'migrated_variant.json'

    def __init__(self):
        self.records = set()
        self.read_from_json()
        print(f'INFO: VariantMigrator has {len(self.records)} records so far')
    
    def read_from_json(self):
        p = pathlib.Path(self.RECORD_FILENAME)
        if not p.is_file():
            return set()
        with open(self.RECORD_FILENAME, 'r') as f:
            records = json.load(f)
            if not isinstance(records, list):
                raise Exception('ReadJsonError: json content is not a list')
            self.records = set(records)    
    
    def write_to_json(self):
        with open(self.RECORD_FILENAME, 'w') as f:
            json.dump(list(self.records), f)
    
    def exists(self, variant_pk):
        return variant_pk in self.records
    
    def add(self, variant_pk):
        self.records.add(variant_pk)

variant_migrator = VariantMigrator()

def getConnection(config_data,type):
    if (type == 'local'):
        return psycopg2.connect(user=config_data['db']['local']['user'],
                                host=config_data['db']['local']['host'],
                                port=config_data['db']['local']['port'],
                                database=config_data['db']['local']['database'])
    elif (type == 'ec2'):
        return psycopg2.connect(user=config_data['db']['ec2']['user'],
                                password=config_data['db']['ec2']['password'],
                                host=config_data['db']['ec2']['host'],
                                port=config_data['db']['ec2']['port'],
                                database=config_data['db']['ec2']['database'])
    else:
        raise Exception("Bad instance type")

def transform_gdm(row):
    post_data=json.dumps(row[2])
    gene_symbol=row[3]
    diseaseId=row[4]
    post_data = json.loads(post_data)
    iterator = iter(post_data)
    key = next(iterator)
    gdm = post_data[key]
    del gdm['gene']
    gdm['gene']=gene_symbol
    del gdm['disease']
    gdm['disease']=diseaseId
    post_data['body']=gdm       
    post_data=json.dumps(post_data)
    return post_data
def transform_annotation(row):
    post_data=json.dumps(row[2])
    pmid=row[3]
    associatedGdm=row[4]
    post_data = json.loads(post_data)
    iterator = iter(post_data)
    key = next(iterator)
    annotation = post_data[key]
    del annotation['article']
    annotation['article']=pmid
    annotation['associatedGdm']=associatedGdm
    post_data['body']=annotation       
    post_data=json.dumps(post_data)
    return post_data
def transform(row):
    #select item_type, rownum, item, disease_id, 
    #status_p,diseaseterm, provisional_variant, associated_snapshots as snapshots
    #print ("Provisional variant %s " %row)
    
    post_data=json.dumps(row[2])
    #print ("Post%s %s" %(post_data,row[0]))
    if (row[0] == 'user'):
        post_data = json.loads(post_data)
        iterator = iter(post_data)
        key = next(iterator)
        user = post_data[key]
        user ['affiliations']=user['affiliation']
        del user['affiliation']
        if isinstance(user['affiliations'], list):
            user['affiliations'] = list(filter(lambda aff: str(aff).strip() != '', user['affiliations']))
        else:
            raise Exception(f'UserMigrationError: `affiliations` is not a list: {user}')
        user ['given_name']=user['first_name']
        user ['name']=user['first_name']
        user ['family_name']=user['last_name']
        del user['first_name']
        del user['last_name']
        del user['given_name']
        # user ['institution'] = 'STN'
        # user ['phone_number'] = '15556667777'
        # user ['phone_number_verified'] = 'true'
        post_data['body']=user 
        post_data = json.dumps(post_data)
    if (row[0] == 'interpretation'):
        post_data = json.loads(post_data)
        iterator = iter(post_data)
        key = next(iterator)
        interpretation = post_data[key]
        interpretation['disease']=row[3]
        interpretation['status']=row[4]
        interpretation['diseaseTerm']=row[5]
        provisional_variant=row[6]
        snapshots=row[7]
        #print ("Provisional variant %s " %provisional_variant)
        #print ("Snapshot %s " %snapshots)
        if bool(provisional_variant):
            print ("Provisional variant exists")
            interpretation['provisionalVariant']=provisional_variant
        if bool(snapshots):
            print ("Snapshots exists")
            interpretation['snapshots']=snapshots
        if 'extra_evidence_list' in interpretation: 
            interpretation['curated_evidence_list'] = interpretation['extra_evidence_list']
            del interpretation ['extra_evidence_list']
        if 'provisional_variant' in interpretation:
            del interpretation ['provisional_variant']
        post_data['body']=interpretation 
        post_data = json.dumps(post_data)
    elif row[0] == 'curated-evidence':
        has_articles=row[3]
        #print ('In curated evidence has article %s' %has_articles)
        if (has_articles =='Y'):
            post_data = json.loads(post_data)
            iterator = iter(post_data)
            key = next(iterator)
            curated_evidence = post_data[key]
            del curated_evidence['articles']
            curated_evidence['articles']=post_data['articles']
            post_data['body']=curated_evidence
            del post_data['articles']
            post_data=json.dumps(post_data)    
    elif row[0] == 'gdm':
        post_data=transform_gdm(row)
    elif row[0] == 'annotation':
        post_data=transform_annotation(row)
    elif row[0] == 'snapshot':
        post_data = json.loads(post_data)
        #print ("Snapshot before transform %s " %post_data)
        iterator = iter(post_data)
        key = next(iterator)
        snapshot = post_data[key]
        interpretation=snapshot['resourceParent']['interpretation']['uuid']
        snapshot['interpretation']=interpretation
        if 'disease' in snapshot['resourceParent']['interpretation']:
            if 'diseaseId' in snapshot['resourceParent']['interpretation']['disease']:
                snapshot['disease']= snapshot['resourceParent']['interpretation']['disease'] ['diseaseId']
            if 'term' in snapshot['resourceParent']['interpretation']['disease']:
                snapshot['diseaseTerm']=snapshot['resourceParent']['interpretation']['disease']['term']
        del snapshot ['resourceParent']
        del snapshot ['resource']['@type']
        del snapshot ['resource']['@id']
        del snapshot ['resource']['uuid']
        resourceParent={}
        resourceParent['interpretation']=interpretation
        snapshot['resourceParent']=resourceParent
        #print ("Provisional variant %s " %provisional_variant)
        #print ("Snapshot %s " %post_data)
        post_data=json.dumps(post_data)
    
    return post_data  

def execute(items, base_url,threads):
    print(f'INFO: got sql results {len(items)}, baseUrl={base_url}, threads={threads}, executing...\n')
    try:
        requests = []
        print('INFO: dispatching FuturesSession...')
        session = FuturesSession(executor=ThreadPoolExecutor(max_workers=threads))
        print('INFO: authing AWS...')
        auth=AWS4Auth(os.getenv('AWS_ACCESS_KEY_ID'), \
            os.getenv('AWS_SECRET_ACCESS_KEY'),'us-west-2', \
                'execute-api')
        info('execute')
        print('Number of items = %s ' % len(items))
        for row in items:
            item_type = row[0]
            item_counter=row[1]
            # if already processed, skip it
            body = row[2]['body']
            if 'PK' in body:
                pk = body['PK']
            elif 'rid' in body:
                pk = body['rid']
            else:
                raise Exception(f'ParsePKError: no rid nor PK on object: {body}')
            if variant_migrator.exists(pk):
                continue

            post_data = transform (row)
            api_endpoint = base_url+ item_type
            if (item_type != 'computational' and item_type != 'experimental'\
                and item_type != 'casecontrol' and item_type != 'evidencescore'\
                and item_type not in ('functional', 'pathogenicity')  ):
                api_endpoint = base_url+ item_type + "s"
                if (item_type == 'snapshot'):
                    api_endpoint += "/?type=interpretation&action=provision"
            if (item_type == 'family'):
                api_endpoint = base_url+ 'families'
             
            # print('INFO: API end point = %s ' %api_endpoint)
            # print('INFO: data = %s ' %post_data)
            request = session.post(api_endpoint,auth=auth, data=post_data )
            request.index = item_counter
            request.migrated_object_body = body
            request.migrated_object_pk = pk
            requests.append(request)
        
        for request in as_completed(requests):
            response = request.result()
            status_code=str(response.status_code)
            if (status_code != '201'):
                error_message = 'End: ***Error***' + str(request.index) + " Status code " + str(status_code) + f' {response.text}' + '\nObject: ' + str(request.migrated_object_body) + '\n\n'
                print(error_message)
                error_tracker.log(error_message)
                error_tracker.save()
            else:
                print('End: ' + str(request.index) + " Status code " + str(status_code))
                variant_migrator.add(request.migrated_object_pk)
                variant_migrator.write_to_json()

        
    except psycopg2.Error as error:
        info('(ERROR) execute')
        print(f'Postgres error {error}')
        traceback.print_tb(error.__traceback__)
    except Exception as error:
        info('(ERROR) execute')
        print('Error during post: %s' %error, f', last post_data: {post_data if post_data else "None"}' )
        error_tracker.save()
        raise error
    
    if len(items) != len(variant_migrator.records):
        print('WARNING: some records are not yet processed')
    else:
        print('INFO: all records in specified range are processed successfully!')
    
    error_tracker.save()

def info(title):
    print(title)
    print('module name:', __name__)
    if hasattr(os, 'getppid'):  # only available on Unix
        print('parent process:', os.getppid())
    print('process id:', os.getpid())

def chunk(sql, config_data, base_url,threads,data, inst_type):
    print(f'INFO: getting postgres connection...\nconfig={config_data}\n\n')
    connection = getConnection(config_data,inst_type)
    print(f'INFO: connected to postgres...')
    try:
        cursor = connection.cursor()
        print ("Query = %s data = %s" %(sql, data))
        cursor.execute(sql,data)
        items=cursor.fetchall()
        execute(items,base_url,threads)
        #print(items)
    except (Exception, psycopg2.Error) as error:
        print('Error while fetching data from PostgreSQL %s' %error)
    finally:
        connection.close()

def main():
    if len(sys.argv) < 7:
        print('Usage : python migrate.py <config_file> <#threads> <item_type> <start> <end> <inst_type>s')
        sys.exit(1)
    file = sys.argv[1]
    threads = int(sys.argv[2])
    item_type = sys.argv[3]
    start = int(sys.argv[4])
    end = int(sys.argv[5])
    inst_type = sys.argv[6]
    if (item_type == 'custom'):
        data=(start,end)
    else:
        data=(item_type, start,end)
    with open(file, 'r') as stream:
        config_data = yaml.load(stream, Loader=yaml.FullLoader)
    base_url = config_data['endpoint']['url']
    queries = config_data['queries']
    for query in queries:
        print(f'INFO: running sql query...\n{query}\n\n')
        sql = query + " where rownum >= %s and rownum < %s "
        chunk (sql,config_data,base_url,threads,data, inst_type)
if __name__=='__main__':
    main()