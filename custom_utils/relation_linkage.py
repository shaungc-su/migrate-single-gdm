from src.utils.dict import dictdeepget, dictdeepset


LINKAGE_TRANSFORM = {
    'article': 'pmid',
    'disease': 'diseaseId',
    'gene': 'symbol'
}

def is_snapshot(item: dict):
    if item.get('item_type') == 'snapshot':
        return True

    # if the following field exists, we decide it's a snapshot object
    # that is directly embedded in its parent
    return item.get('resourceType') and item.get('resourceId')

def get_item_type(item: dict):
    if item.get('item_type'):
        return item['item_type']
    if item.get('@type'):
        return item['@type'][0]
    if is_snapshot(item):
        return 'snapshot'
    
    raise Exception(f'Cannot determine item type for item: {item[:1000]}...')

def get_pk_or_rid(item):
    item_type = item['item_type']
    if item_type in LINKAGE_TRANSFORM:
        return item[LINKAGE_TRANSFORM[item_type]]
    
    # possible fields that provides uuid4 style id is rid, PK or uuid
    
    pk = item.get('rid')
    if pk:
        return pk
    
    pk = item.get('PK')
    if pk:
        return pk
    
    return item['uuid']

class TransformWork:
    def __init__(self, parent_item_type, parent_rid, parent_field_name, relation_item_type):
        '''
            e.g. refs in family.commonDiagnosis needs to be transformed,
            so we call TransformWork('family', familyRid, 'commonDiagnosis', 'disease')
        '''
        self.parent_item_type = parent_item_type
        self.parent_rid = parent_rid
        self.parent_field_name = parent_field_name
        self.relation_item_type = relation_item_type

class RelationLinkageTransformer:
    def __init__(self, logger):
        self.list_to_transform = []
        self.logger = logger
    
    def add(self, parent_item_type, parent_rid, parent_field_name, relation_item_type):
        '''
            e.g. refs in family.commonDiagnosis needs to be transformed,
            so we call .add('family', familyRid, 'commonDiagnosis', 'disease')
        '''
        if relation_item_type in LINKAGE_TRANSFORM:
            self.list_to_transform.append(TransformWork(
                parent_item_type, parent_rid, parent_field_name, relation_item_type
            ))
    
    def processAll(self, object_store_manager):
        self.logger.info('transforming relation linkages...')
        for transform_work in self.list_to_transform:
            parent = {**object_store_manager.get(transform_work.parent_item_type, transform_work.parent_rid)}
            parent_field_value = dictdeepget(parent, transform_work.parent_field_name)
            
            if isinstance(parent_field_value, str):
                related_item_rid = parent_field_value
                related_item = object_store_manager.get(transform_work.relation_item_type, related_item_rid)
                dictdeepset(parent, transform_work.parent_field_name, get_pk_or_rid(related_item))
                self.logger.debug(f'transforming linkage, {transform_work.parent_item_type}({transform_work.parent_rid}).{transform_work.parent_field_name}({related_item_rid}->{get_pk_or_rid(related_item)})')
            elif isinstance(parent_field_value, list) and len(parent_field_value) > 0:
                related_item_rids = parent_field_value
                related_items = [object_store_manager.get(transform_work.relation_item_type, related_item_rid) for related_item_rid in related_item_rids]
                dictdeepset(parent, transform_work.parent_field_name, [
                    get_pk_or_rid(related_item) for related_item in related_items
                ])
                self.logger.debug(f'transforming linkage, {transform_work.parent_item_type}({transform_work.parent_rid}).{transform_work.parent_field_name}([{related_item_rids[0]}->{get_pk_or_rid(related_items[0])}, ...])')
            
            object_store_manager.insert(parent)
