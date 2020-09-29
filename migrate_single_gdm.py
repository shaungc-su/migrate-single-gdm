import psycopg2
import yaml
import json
from utils.schema import SchemaManager
from utils.logger import Logger
from utils.object_store import ObjectStoreManager
from utils.sls import Serverless, DynamoDB
from utils.relation_linkage import RelationLinkageTransformer, get_pk_or_rid

# needs to acticate venv, navigate to `gci-vci-serverless/src`, create a setup.py with content below, and run `pip install .`
# from setuptools import setup, find_packages
# setup(name='gcivcisls', version='1.0', packages=find_packages())
from models.schema_reader import is_singular_relational_field, is_plural_relational_field

# RELN - Rao's largest gdm
GDM_RID = 'a6c35a84-e7c2-4b8c-9697-afb4b07e2521'
# Howard's
# GDM_RID = '1c767179-c29a-483f-9cab-791a0e7960d4'
with open('config_recent.yaml', 'r') as stream:
    config_data = yaml.load(stream, Loader=yaml.FullLoader)
logger = Logger()
schema_manager = SchemaManager()
object_store_manager = ObjectStoreManager(gdm_rid=GDM_RID)
sls = Serverless(logger, base_url=config_data['endpoint']['url'])
db = DynamoDB(logger)
relation_linkage_transformer = RelationLinkageTransformer(logger)

def getConnection():
    type = 'local'
    logger.info('Getting connection object')
    if (type == 'local'):
        return psycopg2.connect(user=config_data['db']['local']['user'],
                                password=config_data['db']['local']['password'],
                                host=config_data['db']['local']['host'],
                                port=config_data['db']['local']['port'],
                                database=config_data['db']['local']['database'])
    elif (type == 'ec2'):
        logger.debug(f"Connecting to ec2 postgres, psw={config_data['db']['ec2']['password']}")
        return psycopg2.connect(user=config_data['db']['ec2']['user'],
                                password=config_data['db']['ec2']['password'],
                                host=config_data['db']['ec2']['host'],
                                port=config_data['db']['ec2']['port'],
                                database=config_data['db']['ec2']['database'])
    else:
        raise Exception("Bad instance type")

def sql_fetchall(item_type, rid, connection):
    data = (item_type, rid)
    sql_query = '''
        SELECT item_type, rownum, item FROM (
            SELECT row_number() over(order by rid, sid) as rownum,
            item_type, 
            item
            FROM migrate_recent_items 
            WHERE item_type=%s AND rid=%s
        )a
    '''

    try:
        logger.debug('Getting cursor')
        cursor = connection.cursor()
        logger.info(f'executing query {item_type}...')
        cursor.execute(sql_query, data)
        items = cursor.fetchall()
        objects = list(map(lambda item: item[2]['body'], items))
        return objects
    except psycopg2.Error as error:
        print('Error while fetching data from PostgreSQL: %s' % error)
        # postgres will abort following transaction anyways, so just close it
        connection.close()
        raise

def generic_transformation(parent):
    new_parent = {**parent}
    item_type = parent['item_type']

    if item_type == 'annotation':
        new_parent['associatedGdm'] = GDM_RID

    # article, disease and gene PK will be handled at sls controller level when create()
    # so no need to transform (i.e. rid->PK, etc)
    
    # while sls controller does transform PK / other fields correctly for us,
    # it does not transform reference to those objects reside in other objects
    # hence we need to do it ourself - the `RelationLinkageTransformer` handles it
    
    return new_parent

def collect_gdm_related_objects(gdm_rid):
    connection = getConnection()

    # a dfs traverse through the relation graph
    store = {}
    object_stack = [{
        'item_type': 'gdm',
        'rid': gdm_rid
    }]

    processed_counter = 0
    while object_stack:
        object_meta = object_stack.pop()
        item_type = object_meta['item_type']
        related_rid = object_meta['rid']

        if object_store_manager.exist(item_type, related_rid):
            parent = object_store_manager.get(item_type, related_rid)
            logger.debug(f'Found in store so reuse: {item_type} {related_rid}')
        else:
            # fetch object from postgres
            try:
                items = sql_fetchall(item_type, related_rid, connection)
            except psycopg2.Error as error:
                if 'invalid input syntax for type uuid' in str(error):
                    # means relation links are transformed -
                    # since we only do transform after collecting all related objects
                    # this means objects are already cached by object store manager
                    # hence no need to sql fetch
                    logger.info(f'sql processed #{processed_counter} (skipped due to relation link already transformed)')
                    connection = getConnection()
                    continue
                raise

            if len(items) != 1:
                raise Exception(f'{item_type} queried by PK/rid `{related_rid}` but returned not one: {items}')
            parent = items[0]
            logger.debug(f'fetched items = {parent["item_type"]} {get_pk_or_rid(parent)}')

            # store it (the normalized form in postgres)
            parent = generic_transformation(parent)
            object_store_manager.insert(parent)

        schema = schema_manager.read_schema_from_file(item_type)

        # visit related fields
        for field_name, field_value in parent.items():
            if field_name in schema:
                # get schema e.g. gdm.diseae = { $schema: disease, type: object }
                related_field_schema = schema[field_name]
                if is_singular_relational_field(related_field_schema):
                    object_stack.append({
                        'item_type': related_field_schema['$schema'],
                        'rid': parent[field_name]
                    })
                    relation_linkage_transformer.add(
                        parent_item_type=item_type,
                        parent_rid=parent['rid'],
                        parent_field_name=field_name,
                        relation_item_type=related_field_schema['$schema']
                    )
                elif is_plural_relational_field(related_field_schema):
                    for related_rid in parent[field_name]:
                        object_stack.append({
                            'item_type': related_field_schema['items']['$schema'],
                            'rid': related_rid
                        })
                    relation_linkage_transformer.add(
                        parent_item_type=item_type,
                        parent_rid=parent['rid'],
                        parent_field_name=field_name,
                        relation_item_type=related_field_schema['items']['$schema']
                    )
                    
        processed_counter += 1
        logger.info(f'sql processed #{processed_counter}')
    
        object_store_manager.save()

    connection.close()

def transform_relation_links():
    '''
        If a schema's PK is transformed (e.g. disease: uuid -> diseaseId),
        transform all references in other objects accordingly
    '''
    relation_linkage_transformer.processAll(object_store_manager)
    object_store_manager.save()

def post_related_objects():
    items = object_store_manager.getAll(prioritized_schema_list=[
        # create objects that does not have relationship first
        'user', 'disease', 'article', 'gene', 'evidenceScore',
        # create evidence objects
        'individual', 'family', 'group', 'experimental', 'caseControl',
        # gdm should be the last, so that related fields can populate properly
        'annotation', 'gdm'
    ])
    logger.debug(f'n = {len(items)}')

    for index, item in enumerate(items):
        # use sls to GET
        db_item_already = sls.get(item)
        # use dynamodb to GET
        # db_item_already = db.get(item)
        if not db_item_already:
            res = sls.post(item)
            logger.info(f'POST {res.status_code} {item["item_type"]}({get_pk_or_rid(item)}) processed {index+1}/{len(items)} item')
        else:
            logger.info(f'POST skipping {item["item_type"]}({get_pk_or_rid(item)}) processed {index+1}/{len(items)} item')
        
if __name__ == "__main__":
    # db.reset()
    
    collect_gdm_related_objects(gdm_rid=GDM_RID)
    transform_relation_links()
    post_related_objects()