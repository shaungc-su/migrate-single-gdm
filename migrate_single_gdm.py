import psycopg2
import yaml
import json
import copy
import pathlib
from custom_utils.config import config_data
from custom_utils.sql import getConnection
from custom_utils.logger import Logger
from custom_utils.object_store import ObjectStoreManager
from custom_utils.sls import SLS, DYNAMODB
from custom_utils.relation_linkage import RelationLinkageTransformer, get_pk_or_rid, is_snapshot, get_item_type

# needs to acticate venv, navigate to `gci-vci-serverless/src`, create a setup.py with content below, and run `pip install .`
# from setuptools import setup, find_packages
# setup(name='gcivcisls', version='1.0', packages=find_packages())
from src.models.schema_reader import is_singular_relational_field, is_plural_relational_field, parse_schema_string
from src.models.item_type_serializer import ModelSerializer
from src.utils.dict import dictdeepget

logger = Logger
sls = SLS
db = DYNAMODB

def sql_fetchall(item_type, rid, connection):
    if not (isinstance(item_type, str) and isinstance(rid, str)):
        raise Exception(f'SQLFetchAllError: invalid arg type, item_type={item_type}, rid={rid}')

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

def generic_transformation(gdm_rid, parent, item_type=None):
    new_parent = {**parent}

    if not item_type:
        item_type = parent.get('item_type')

    if item_type == 'annotation':
        new_parent['associatedGdm'] = gdm_rid

    # article, disease and gene PK will be handled at sls controller level when create()
    # so no need to transform (i.e. rid->PK, etc)
    
    # while sls controller does transform PK / other fields correctly for us,
    # it does not transform reference to those objects reside in other objects
    # hence we need to do it ourself - the `RelationLinkageTransformer` handles it
    
    # snapshot also has to be treated specially
    if item_type == 'snapshot':
        new_parent['item_type'] = 'snapshot'
        new_parent['PK'] = new_parent.get('uuid', new_parent['rid'])
        
        # assign PK and item_type by using graph traverse

        stack = []
        
        gdm = new_parent['resourceParent'].get('gdm')
        if gdm:
            stack.append(gdm)
            # gdm does not embed annotations, so we need to add them to our traverse stack manually
            if gdm.get('annotations'):
                stack.extend(gdm['annotations'])
        resource = new_parent.get('resource')
        if resource:
            stack.append(resource)

        while stack:
            local_parent = stack.pop()

            # skip string value which is already PK and not embedded, so no need to go in there do transform
            if isinstance(local_parent, str):
                continue

            # assign values
            local_parent['item_type'] = get_item_type(local_parent)
            local_parent['PK'] = get_pk_or_rid(local_parent)

            Model = ModelSerializer._get_model(None, local_parent['item_type'])
            singular_dot_representation_keys, plural_dot_representation_keys = Model.get_dot_representation_keys()
            for dot_representation in singular_dot_representation_keys:
                stack.append(dictdeepget(local_parent, dot_representation))
            for dot_representation in plural_dot_representation_keys:
                stack.extend(dictdeepget(local_parent, dot_representation, []))
                    
    return new_parent

def collect_gdm_related_objects(gdm_rid, object_store_manager, relation_linkage_transformer):
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
        # in case the parent is embedded directly on its ancestor, like snapshot (in legacy db)
        parent = object_meta.get('object')

        if object_store_manager.exist(item_type, related_rid):
            parent = object_store_manager.get(item_type, related_rid)
            logger.debug(f'Found in store so reuse: {item_type} {related_rid}')
        elif not parent:
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
        else:
            # in case the parent is embedded directly on its ancestor
            # 
            # but this is unlikely what we want - even like snapshot nested stored in associatedClassificationSnapshots (in legacy db),
            # it does not have complete snapshot object, i.e. resourceParent is stripped off.
            # so just query by uuid/rid/PK is preferrable, don't directly use the nested object
            pass

        # store it (the normalized form in postgres)
        parent = generic_transformation(gdm_rid, parent, item_type=item_type)
        object_store_manager.insert(parent)


        Model = ModelSerializer._get_model(None, item_type)
        singular_dot_representation_keys, plural_dot_representation_keys = Model.get_dot_representation_keys()

        # patch schema of gdm.annotations so that we know how to link gdm to annotation
        # but don't change sls json schema for gdm, since controller
        # use it to populate field, and we don't want to populate gdm.annotations
        if item_type == 'gdm':
            plural_dot_representation_keys.append('annotations')
            Model.map_dot_representation_to_item_type['annotations'] = 'annotation'
            plural_dot_representation_keys.append('variantPathogenicity')
            Model.map_dot_representation_to_item_type['variantPathogenicity'] = 'pathogenicity'

        # visit related fields
        for dot_representation in singular_dot_representation_keys:
            parent_field_value = dictdeepget(parent, dot_representation)
            if not parent_field_value or not isinstance(parent_field_value, str):
                continue

            related_item_type = Model.map_dot_representation_to_item_type[dot_representation]
            object_stack.append({
                'item_type': related_item_type,
                'rid': parent_field_value
            })
            relation_linkage_transformer.add(
                parent_item_type=item_type,
                parent_rid=parent['rid'],
                parent_field_name=dot_representation,
                relation_item_type=related_item_type
            )
            
        for dot_representation in plural_dot_representation_keys:
            parent_field_value = dictdeepget(parent, dot_representation)
            if not parent_field_value or not isinstance(parent_field_value, list):
                continue

            related_item_type = Model.map_dot_representation_to_item_type[dot_representation]
            for related_rid in parent_field_value:
                if isinstance(related_rid, str):
                    object_stack.append({
                        'item_type': related_item_type,
                        'rid': related_rid
                    })
                elif isinstance(related_rid, dict) and (
                    is_snapshot(related_rid)
                ):
                    # snapshot does not have rid nor item_type in legacy system
                    snapshot = related_rid
                    object_stack.append({
                        'item_type': related_item_type,
                        
                        # looks like some snapshot does not have rid
                        'rid': snapshot['uuid'],

                        # do not use nested snapshot - it does not have resourceParent - 
                        # use its uuid/rid to query the actual 'complete' snapshot object in db
                        # 'object': snapshot
                    })
                else:
                    raise Exception(f'Error: array field item has invalid type (neither PK (str) or object (dict)) in {item_type}.{dot_representation}, related_rid={related_rid}')

            relation_linkage_transformer.add(
                parent_item_type=item_type,
                parent_rid=parent['rid'],
                parent_field_name=dot_representation,
                relation_item_type=related_item_type
            )

        processed_counter += 1
        logger.info(f'sql processed #{processed_counter} (not saved yet)')
    
    logger.info('saving all sql process results to file...')
    object_store_manager.save()

    connection.close()

def transform_relation_links(object_store_manager, relation_linkage_transformer):
    '''
        If a schema's PK is transformed (e.g. disease: uuid -> diseaseId),
        transform all references in other objects accordingly
    '''
    relation_linkage_transformer.processAll(object_store_manager)
    object_store_manager.save()

def post_related_objects(object_store_manager):
    items = object_store_manager.getAll(prioritized_schema_list=[
        # create objects that does not have relationship first
        'user', 'disease', 'article', 'gene', 'evidenceScore', 'snapshot', 'provisionalClassification', 'assessment',
        # VCI objects
        'variant',
        # create evidence objects
        'individual', 'family', 'group', 'experimental', 'caseControl', 'pathogenicity',
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

def single_migrate(gdm_rid):
    object_store_manager = ObjectStoreManager(gdm_rid=gdm_rid)
    relation_linkage_transformer = RelationLinkageTransformer(logger)

    collect_gdm_related_objects(gdm_rid=gdm_rid, object_store_manager=object_store_manager, relation_linkage_transformer=relation_linkage_transformer)
    transform_relation_links(object_store_manager=object_store_manager, relation_linkage_transformer=relation_linkage_transformer)
    post_related_objects(object_store_manager)

if __name__ == "__main__":
    # note that this will cause local dynamodb data file to change
    # which may require a re-start of the dynamodb server
    # better reset the db by replacing the data file manually
    # db.reset()
    
    # RELN - Rao's largest gdm
    # scale statistics: sql process 923, item to POST 327
    RELN_GDM_RID = 'a6c35a84-e7c2-4b8c-9697-afb4b07e2521'
    single_migrate(RELN_GDM_RID)
    
    # # Howard's
    # HO_GDM_RID = '1c767179-c29a-483f-9cab-791a0e7960d4'
    # single_migrate(HO_GDM_RID)

    # A hearing loss GDM that is approved, published and new provisional
    # SYNE4
    # HEARING_LOSS_GDM_RID = '429e5749-b39c-4917-b7db-00268456c59d'
    # single_migrate(HEARING_LOSS_GDM_RID)

    # For testing JIRA-365
    # SNAPSHOT_BUG_GDM_RID = '0683943f-da80-4a3c-8f9f-454d59040eb2'
    # single_migrate(SNAPSHOT_BUG_GDM_RID)