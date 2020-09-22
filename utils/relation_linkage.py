LINKAGE_TRANSFORM = {
    'article': 'pmid',
    'disease': 'diseaseId',
    'gene': 'symbol'
}

def get_pk_or_rid(item):
    item_type = item['item_type']
    if item_type in LINKAGE_TRANSFORM:
        return item[LINKAGE_TRANSFORM[item_type]]
    return item['rid']

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

            if isinstance(parent[transform_work.parent_field_name], str):
                related_item_rid = parent[transform_work.parent_field_name]
                related_item = object_store_manager.get(transform_work.relation_item_type, related_item_rid)
                parent[transform_work.parent_field_name] = get_pk_or_rid(related_item)
                self.logger.debug(f'transforming linkage, {transform_work.parent_item_type}({transform_work.parent_rid}).{transform_work.parent_field_name}({related_item_rid}->{get_pk_or_rid(related_item)})')
            elif isinstance(parent[transform_work.parent_field_name], list) and len(parent[transform_work.parent_field_name]) > 0:
                related_item_rids = parent[transform_work.parent_field_name]
                related_items = [object_store_manager.get(transform_work.relation_item_type, related_item_rid) for related_item_rid in related_item_rids]
                parent[transform_work.parent_field_name] = [
                    get_pk_or_rid(related_item) for related_item in related_items
                ]
                self.logger.debug(f'transforming linkage, {transform_work.parent_item_type}({transform_work.parent_rid}).{transform_work.parent_field_name}([{related_item_rids[0]}->{get_pk_or_rid(related_items[0])}, ...])')
            
            object_store_manager.insert(parent)